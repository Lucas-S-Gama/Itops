"""
Captura de métricas de firewalls (simulados) a cada 1 minuto.

Coleta:
  - ID_firewall      : alterna entre os firewalls configurados (um por ciclo)
  - Active_sessions  : número de sessões TCP/UDP ativas (psutil.net_connections),
                       com fallback para valor simulado se faltar permissão
  - Dropped_packets  : pacotes descartados (dropin + dropout de psutil.net_io_counters())
  - Top_blocked_IP   : IP SIMULADO tentando acesso forçado (ataque DoS) e bloqueado
  - CPU/RAM_Usage    : uso de CPU e RAM em percentual
  - Bytes_Sent/Recv  : contadores de rede via psutil.net_io_counters()

Armazenamento (AWS S3):
  - Bucket e prefixo ("pasta") configurados abaixo
  - Um objeto JSON por dia PARA CADA firewall, no formato:
        <PREFIXO_S3>/AAAA-MM-DD_HH-MM_fwXX.json   (ex.: firewalls/2026-04-06_10-30_fw01.json)
    O HH-MM é o horário da primeira captura daquele firewall no dia, quando o
    objeto é criado. Nas capturas seguintes do mesmo dia, os registros são
    adicionados ao mesmo objeto. Ao virar o dia, novos objetos são criados.

Para encerrar, use Ctrl+C.
"""

import io
import itertools
import random
import time
from datetime import datetime
from dotenv import load_dotenv 
import os
import boto3
import pandas as pd
import psutil
from botocore.exceptions import ClientError

load_dotenv()


# ----------------------------- Configurações ------------------------------
# AWS  (preencha com as informações corretas)
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIXO_S3 = os.getenv("PREFIXO_S3")

FIREWALLS = ["fw01"]  # ordem de alternância entre os firewalls
INTERVALO_SEGUNDOS = 60            # loop de 1 em 1 minuto

# Cliente S3
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)
s3_client = session.client("s3")


# ------------------------------- Coleta -----------------------------------
def contar_sessoes_ativas() -> tuple[int, bool]:
    """
    Conta as sessões TCP/UDP ativas. Retorna (quantidade, simulado).
    Em alguns sistemas (ex.: macOS) é preciso permissão de administrador;
    nesse caso o valor é simulado.
    """
    try:
        conexoes = psutil.net_connections(kind="inet")  # inet = TCP + UDP (IPv4/IPv6)
        return len(conexoes), False
    except (psutil.AccessDenied, PermissionError):
        return random.randint(200, 5000), True


def gerar_ip_atacante() -> str:
    """Simula um IP externo tentando acesso forçado (DoS) ao firewall."""
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def coletar_metricas(id_firewall: str) -> dict:
    """Coleta uma amostra das métricas e devolve em formato de dicionário."""
    rede = psutil.net_io_counters()
    sessoes, simulado = contar_sessoes_ativas()

    return {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ID_firewall": id_firewall,
        "Active_sessions": sessoes,
        "Sessions_simulated": simulado,
        "Dropped_packets": rede.dropin + rede.dropout,
        "Top_blocked_IP": gerar_ip_atacante(),
        # interval=1 mede o uso real da CPU durante 1 segundo
        "CPU_Usage": psutil.cpu_percent(interval=1),
        "RAM_Usage": psutil.virtual_memory().percent,
        "Bytes_Sent": rede.bytes_sent,
        "Bytes_Recv": rede.bytes_recv,
    }


# ----------------------------- Armazenamento ------------------------------
def obter_chave_do_dia(id_firewall: str) -> str:
    """
    Retorna a chave (caminho no bucket) do JSON do dia atual para o firewall.
    Se já existir um objeto de hoje para ele, reutiliza; senão, define uma
    nova chave com o horário atual (PREFIXO/AAAA-MM-DD_HH-MM_fwXX.json).
    """
    agora = datetime.now()
    data_hoje = agora.strftime("%Y-%m-%d")

    prefixo_busca = f"{PREFIXO_S3}/{data_hoje}_"
    sufixo = f"_{id_firewall}.json"

    existentes = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefixo_busca):
        for obj in pagina.get("Contents", []):
            if obj["Key"].endswith(sufixo):
                existentes.append(obj["Key"])

    if existentes:
        return sorted(existentes)[0]

    nome = f"{data_hoje}_{agora.strftime('%H-%M')}_{id_firewall}.json"
    return f"{PREFIXO_S3}/{nome}"


def ler_json_s3(chave: str) -> pd.DataFrame | None:
    """Lê o JSON do S3 e devolve um DataFrame. Retorna None se o objeto não existir."""
    try:
        resposta = s3_client.get_object(Bucket=BUCKET_NAME, Key=chave)
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    conteudo = resposta["Body"].read().decode("utf-8")
    return pd.read_json(io.StringIO(conteudo), orient="records", dtype=False)


def salvar_json(registro: dict) -> tuple[str, int]:
    """Adiciona o registro ao JSON do dia do firewall no S3. Retorna (chave, total de linhas)."""
    chave = obter_chave_do_dia(registro["ID_firewall"])
    novo = pd.DataFrame([registro])

    try:
        historico = ler_json_s3(chave)
    except ValueError:
        print("[AVISO] Arquivo JSON inválido/vazio. Recriando o arquivo...")
        historico = None

    if historico is not None:
        df = pd.concat([historico, novo], ignore_index=True)
    else:
        print(f"[INFO] Novo dia/arquivo. Criando 's3://{BUCKET_NAME}/{chave}'...")
        df = novo

    df["Timestamp"] = df["Timestamp"].astype(str)
    corpo = df.to_json(orient="records", indent=4, force_ascii=False)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=chave,
        Body=corpo.encode("utf-8"),
        ContentType="application/json",
    )
    return chave, len(df)


# ------------------------------ Execução ----------------------------------
def main() -> None:
    print("=" * 60)
    print(" Monitoramento de firewalls iniciado")
    print(f" Firewalls (em rodízio): {' -> '.join(FIREWALLS)}")
    print(f" Intervalo: {INTERVALO_SEGUNDOS}s | Destino: s3://{BUCKET_NAME}/{PREFIXO_S3}/")
    print(" Pressione Ctrl+C para encerrar.")
    print("=" * 60)

    rodizio = itertools.cycle(FIREWALLS)
    ciclo = 0
    try:
        while True:
            ciclo += 1
            inicio = time.time()
            id_firewall = next(rodizio)
            print(f"\n[CICLO {ciclo}] Firewall selecionado: {id_firewall}. Iniciando coleta...")

            registro = coletar_metricas(id_firewall)
            origem = "simuladas" if registro["Sessions_simulated"] else "reais"
            print(
                f"[SESSÕES] Ativas TCP/UDP ({origem}): {registro['Active_sessions']} | "
                f"Pacotes descartados: {registro['Dropped_packets']}"
            )
            print(
                f"[ALERTA]  Possível DoS - IP bloqueado: {registro['Top_blocked_IP']}"
            )
            print(
                f"[SISTEMA] CPU: {registro['CPU_Usage']}% | RAM: {registro['RAM_Usage']}%"
            )
            print(
                f"[REDE]    Enviados: {registro['Bytes_Sent']:,} bytes | "
                f"Recebidos: {registro['Bytes_Recv']:,} bytes"
            )

            chave, total = salvar_json(registro)
            print(
                f"[SALVO]   {id_firewall} gravado em 's3://{BUCKET_NAME}/{chave}' "
                f"(total no dia: {total})."
            )

            # Desconta o tempo gasto na coleta para manter o intervalo de ~60s
            espera = 60 - (time.time() % 60)
            print(f"[AGUARDANDO] Próximo ciclo em {espera:.0f} segundos...")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\n[ENCERRADO] Monitoramento interrompido pelo usuário. Até logo!")
    except Exception as erro:
        print(f"\n[ERRO] Falha inesperada: {erro}")


if __name__ == "__main__":
    main()
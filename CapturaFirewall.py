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

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")
PREFIXO_S3 = os.getenv("PREFIXO_S3")

FIREWALLS = ["fw01"]
INTERVALO_SEGUNDOS = 60

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
        conexoes = psutil.net_connections(kind="inet")
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
        "CPU_Usage": psutil.cpu_percent(interval=1),
        "RAM_Usage": psutil.virtual_memory().percent,
        "Bytes_Sent": rede.bytes_sent,
        "Bytes_Recv": rede.bytes_recv,
    }


def obter_chave_do_dia(id_firewall: str) -> str:
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
    try:
        resposta = s3_client.get_object(Bucket=BUCKET_NAME, Key=chave)
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise
    conteudo = resposta["Body"].read().decode("utf-8")
    return pd.read_json(io.StringIO(conteudo), orient="records", dtype=False)


def salvar_json(registro: dict) -> tuple[str, int]:
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

            espera = 60 - (time.time() % 60)
            print(f"[AGUARDANDO] Próximo ciclo em {espera:.0f} segundos...")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\n[ENCERRADO] Monitoramento interrompido pelo usuário. Até logo!")
    except Exception as erro:
        print(f"\n[ERRO] Falha inesperada: {erro}")


if __name__ == "__main__":
    main()
"""
Captura de métricas de firewalls (simulados) a cada 1 minuto.

Coleta:
  - ID_firewall      : alterna entre os firewalls fw02, fw01 e fw03 (um por ciclo)
  - Active_sessions  : número de sessões TCP/UDP ativas (psutil.net_connections),
                       com fallback para valor simulado se faltar permissão
  - Dropped_packets  : pacotes descartados (dropin + dropout de psutil.net_io_counters())
  - Top_blocked_IP   : IP SIMULADO tentando acesso forçado (ataque DoS) e bloqueado
  - CPU/RAM_Usage    : uso de CPU e RAM em percentual
  - Bytes_Sent/Recv  : contadores de rede via psutil.net_io_counters()

Armazenamento:
  - Pasta 'firewalls/' (criada automaticamente se não existir)
  - Um arquivo JSON por dia PARA CADA firewall, no formato:
        AAAA-MM-DD_HH-MM_fwXX.json   (ex.: 2026-04-06_10-30_fw01.json)
    O HH-MM é o horário da primeira captura daquele firewall no dia, quando o
    arquivo é criado. Nas capturas seguintes do mesmo dia, os registros são
    adicionados ao mesmo arquivo. Ao virar o dia, novos arquivos são criados.

Para encerrar, use Ctrl+C.
"""

import boto3
import glob
import itertools
import os
import random
import time
from datetime import datetime

import pandas as pd
import psutil

# AWS
session = boto3.Session(
 aws_access_key_id=AWS_ACCESS_KEY_ID,
 aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
 aws_session_token=AWS_SESSION_TOKEN,
 region_name="us-east-1", # Substitua pela sua região se necessário
)
s3_client = session.client("s3")
bucket_name = "seu-nome-do-bucket"
file_key = "caminho/para/o/seu_arquivo.csv"
response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
df = pandas.read_csv(io.BytesIO(response["Body"].read()), sep=";")

# ----------------------------- Configurações ------------------------------
FIREWALLS = ["fw01"]  # ordem de alternância entre os firewalls
PASTA_SAIDA = "firewalls"
INTERVALO_SEGUNDOS = 60            # loop de 1 em 1 minuto


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
def obter_caminho_do_dia(id_firewall: str) -> str:
    """
    Retorna o caminho do JSON do dia atual para o firewall informado.
    Se já existir um arquivo de hoje para ele, reutiliza; senão, define um
    novo com o horário atual no nome (AAAA-MM-DD_HH-MM_fwXX.json).
    """
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    agora = datetime.now()
    data_hoje = agora.strftime("%Y-%m-%d")

    existentes = sorted(
        glob.glob(os.path.join(PASTA_SAIDA, f"{data_hoje}_*_{id_firewall}.json"))
    )
    if existentes:
        return existentes[0]

    nome = f"{data_hoje}_{agora.strftime('%H-%M')}_{id_firewall}.json"
    return os.path.join(PASTA_SAIDA, nome)


def salvar_json(registro: dict) -> tuple[str, int]:
    """Adiciona o registro ao JSON do dia do firewall. Retorna (caminho, total de linhas)."""
    caminho = obter_caminho_do_dia(registro["ID_firewall"])
    novo = pd.DataFrame([registro])

    if os.path.exists(caminho):
        try:
            historico = pd.read_json(caminho, orient="records", dtype=False)
            df = pd.concat([historico, novo], ignore_index=True)
        except ValueError:
            print("[AVISO] Arquivo JSON inválido/vazio. Recriando o arquivo...")
            df = novo
    else:
        print(f"[INFO] Novo dia/arquivo. Criando '{caminho}'...")
        df = novo

    df["Timestamp"] = df["Timestamp"].astype(str)
    df.to_json(caminho, orient="records", indent=4, force_ascii=False)
    return caminho, len(df)


# ------------------------------ Execução ----------------------------------
def main() -> None:
    print("=" * 60)
    print(" Monitoramento de firewalls iniciado")
    print(f" Firewalls (em rodízio): {' -> '.join(FIREWALLS)}")
    print(f" Intervalo: {INTERVALO_SEGUNDOS}s | Pasta: {PASTA_SAIDA}/")
    print(" Pressione Ctrl+C para encerrar.")
    print("=" * 60)

    rodizio = itertools.cycle(FIREWALLS)  # fw02 -> fw01 -> fw03 -> fw02 -> ...
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

            caminho, total = salvar_json(registro)
            print(f"[SALVO]   {id_firewall} gravado em '{caminho}' (total no dia: {total}).")

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
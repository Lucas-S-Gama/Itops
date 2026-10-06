import io
import random
import time
from datetime import datetime

import os
from dotenv import load_dotenv;
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

ANTENAS = ["ap01", "ap02", "ap03"]
INTERVALO_SEGUNDOS = 60

PESOS_BASE = {"ap01": 1.0, "ap02": 0.9, "ap03": 1.1}
VARIACAO_PESO = 0.15
VARIACAO_CONN = 10
VARIACAO_PCT = 5.0

session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    aws_session_token=AWS_SESSION_TOKEN,
    region_name=AWS_REGION,
)
s3_client = session.client("s3")

_acumulado = {ap: {"sent": 0, "recv": 0} for ap in ANTENAS}
_ultimo_host = None


def _limitar_pct(valor: float) -> float:
    return round(min(100.0, max(0.0, valor)), 1)


def _sortear_fracoes() -> dict:
    """Sorteia a fração do tráfego de cada AP (soma = 1), com leve variação."""
    pesos = {
        ap: PESOS_BASE[ap] * random.uniform(1 - VARIACAO_PESO, 1 + VARIACAO_PESO)
        for ap in ANTENAS
    }
    soma = sum(pesos.values())
    return {ap: p / soma for ap, p in pesos.items()}


def _dividir(total: int, fracoes: dict) -> dict:
    """Divide 'total' entre as APs em números inteiros que somam exatamente 'total'."""
    partes, restante = {}, total
    for ap in ANTENAS[:-1]:
        partes[ap] = int(total * fracoes[ap])
        restante -= partes[ap]
    partes[ANTENAS[-1]] = restante
    return partes


def coletar_metricas_todas() -> list[dict]:
    """Coleta as métricas reais uma vez e devolve um registro para cada AP."""
    global _ultimo_host

    rede = psutil.net_io_counters()
    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if _ultimo_host is None:
        delta_sent, delta_recv = rede.bytes_sent, rede.bytes_recv
    else:
        delta_sent = max(0, rede.bytes_sent - _ultimo_host[0])
        delta_recv = max(0, rede.bytes_recv - _ultimo_host[1])
    _ultimo_host = (rede.bytes_sent, rede.bytes_recv)

    fracoes = _sortear_fracoes()
    parte_sent = _dividir(delta_sent, fracoes)
    parte_recv = _dividir(delta_recv, fracoes)

    base_conn = random.randint(20, 100)

    registros = []
    for ap in ANTENAS:
        _acumulado[ap]["sent"] += parte_sent[ap]
        _acumulado[ap]["recv"] += parte_recv[ap]
        registros.append({
            "Timestamp": timestamp,
            "ID_antena": ap,
            "Bytes_Sent": _acumulado[ap]["sent"],
            "Bytes_Recv": _acumulado[ap]["recv"],
            "Active_conn": max(0, base_conn + random.randint(-VARIACAO_CONN, VARIACAO_CONN)),
            "CPU_Usage": _limitar_pct(cpu + random.uniform(-VARIACAO_PCT, VARIACAO_PCT)),
            "RAM_Usage": _limitar_pct(ram + random.uniform(-VARIACAO_PCT, VARIACAO_PCT)),
        })
    return registros


def obter_chave_do_dia(id_antena: str) -> str:
    """
    Retorna a chave (caminho no bucket) do JSON do dia atual para a antena.
    Se já existir um objeto de hoje para ela, reutiliza; senão, define uma
    nova chave com o horário atual (PREFIXO/AAAA-MM-DD_HH-MM_apXX.json).
    """
    agora = datetime.now()
    data_hoje = agora.strftime("%Y-%m-%d")

    prefixo_busca = f"{PREFIXO_S3}/{data_hoje}_"
    sufixo = f"_{id_antena}.json"

    existentes = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefixo_busca):
        for obj in pagina.get("Contents", []):
            if obj["Key"].endswith(sufixo):
                existentes.append(obj["Key"])

    if existentes:
        return sorted(existentes)[0]

    nome = f"{data_hoje}_{agora.strftime('%H-%M')}_{id_antena}.json"
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
    """Adiciona o registro ao JSON do dia da antena no S3. Retorna (chave, total de linhas)."""
    chave = obter_chave_do_dia(registro["ID_antena"])
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


def main() -> None:
    print("=" * 60)
    print(" Monitoramento simultâneo de antenas iniciado")
    print(f" Antenas: {', '.join(ANTENAS)}")
    print(f" Intervalo: {INTERVALO_SEGUNDOS}s | Destino: s3://{BUCKET_NAME}/{PREFIXO_S3}/")
    print(" Pressione Ctrl+C para encerrar.")
    print("=" * 60)

    ciclo = 0
    try:
        while True:
            ciclo += 1
            inicio = time.time()
            print(f"\n[CICLO {ciclo}] Coletando as {len(ANTENAS)} antenas ao mesmo tempo...")

            registros = coletar_metricas_todas()
            soma_sent = 0

            for r in registros:
                soma_sent += r["Bytes_Sent"]
                print(
                    f"[{r['ID_antena'].upper()}] Conexões: {r['Active_conn']:>3} | "
                    f"CPU: {r['CPU_Usage']:>5}% | RAM: {r['RAM_Usage']:>5}% | "
                    f"Enviados: {r['Bytes_Sent']:,} B | Recebidos: {r['Bytes_Recv']:,} B"
                )
                chave, total = salvar_json(r)
                print(f"         salvo em 's3://{BUCKET_NAME}/{chave}' (total no dia: {total})")

            print(f"[TOTAL]  Soma de Bytes_Sent das APs: {soma_sent:,} B")

            espera = 60 - (time.time() % 60)
            print(f"[AGUARDANDO] Próximo ciclo em {espera:.0f} segundos...")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\n[ENCERRADO] Monitoramento interrompido pelo usuário. Até logo!")
    except Exception as erro:
        print(f"\n[ERRO] Falha inesperada: {erro}")


if __name__ == "__main__":
    main()
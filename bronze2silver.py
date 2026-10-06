import argparse
import csv
import io
import json
import re
import time
from datetime import datetime

import boto3
from botocore.exceptions import BotoCoreError, ClientError


AWS_REGION = "us-east-1"
BUCKET_NAME = "itops-gama"
PREFIXO_BRONZE = "01-bronze"
PREFIXO_SILVER = "02-silver"
CHAVE_CSV = f"{PREFIXO_SILVER}/dados_consolidados.csv"


PADRAO_ANTENAS = re.compile(r"_ap\d+\.json$")
PADRAO_FIREWALLS = re.compile(r"_fw\d+\.json$")

INTERVALO_PADRAO_SEG = 180
QTD_APS_ESPERADAS = 3
TOLERANCIA_PCT = 10.0

LIMITE_ACTIVE_CONN = 40
LIMITE_CPU = 80
LIMITE_RAM = 75

COLUNAS = [
    "Timestamp", "Tipo", "ID",
    "Bytes_Sent", "Bytes_Recv", "Delta_Sent", "Delta_Recv", "Intervalo_s",
    "Vazao_Sent_Mbps", "Vazao_Recv_Mbps", "Vazao_Total_Mbps",
    "Active_conn", "Active_sessions", "Dropped_packets", "Top_blocked_IP",
    "CPU_Usage", "RAM_Usage", "status_carga",
    "Minuto_Soma_Conn_APs", "Minuto_FW_Sessoes",
    "Minuto_Soma_Delta_Sent_APs", "Minuto_FW_Delta_Sent",
    "Minuto_Diferenca_Bytes", "Minuto_Desvio_pct", "status_consistencia",
]

s3_client = boto3.client("s3", region_name=AWS_REGION)


# ------------------------------- Leitura ----------------------------------
def interpretar_timestamp(valor):
    """Converte o valor do JSON em datetime. Retorna None se não conseguir."""
    if valor is None:
        return None
    if isinstance(valor, (int, float)):  # epoch em s ou ms
        return datetime.fromtimestamp(valor / 1000 if valor > 1e11 else valor)
    texto = str(valor).strip()
    try:
        return datetime.fromisoformat(texto.replace("Z", ""))
    except ValueError:
        pass
    for formato in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
        try:
            return datetime.strptime(texto, formato)
        except ValueError:
            continue
    return None


def listar_chaves_bronze() -> list[str]:
    """Lista todas as chaves .json do prefixo bronze (ignora o marcador de pasta)."""
    chaves = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for pagina in paginator.paginate(Bucket=BUCKET_NAME, Prefix=f"{PREFIXO_BRONZE}/"):
        for obj in pagina.get("Contents", []):
            if obj["Key"].endswith(".json"):
                chaves.append(obj["Key"])
    return sorted(chaves)


def ler_registros(chaves: list[str], tipo: str) -> list[dict]:
    """Lê os JSONs informados do S3 e devolve registros normalizados."""
    print(f"[LEITURA] s3://{BUCKET_NAME}/{PREFIXO_BRONZE}/ ({tipo}): "
          f"{len(chaves)} arquivo(s) JSON encontrado(s).")

    registros, vistos = [], set()
    for chave in chaves:
        try:
            resposta = s3_client.get_object(Bucket=BUCKET_NAME, Key=chave)
            dados = json.loads(resposta["Body"].read().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, ClientError, BotoCoreError) as erro:
            print(f"[AVISO] Ignorando '{chave}': {erro}")
            continue

        if isinstance(dados, dict):
            dados = [dados]

        for r in dados:
            ts = interpretar_timestamp(r.get("Timestamp"))
            id_disp = r.get("ID_antena") or r.get("ID_firewall") or r.get("ID")
            if ts is None or not id_disp:
                continue
            chave_unica = (id_disp, ts)
            if chave_unica in vistos:
                continue
            vistos.add(chave_unica)

            registros.append({
                "ts": ts,
                "Tipo": tipo,
                "ID": id_disp,
                "Bytes_Sent": r.get("Bytes_Sent"),
                "Bytes_Recv": r.get("Bytes_Recv"),
                "Active_conn": r.get("Active_conn"),
                "Active_sessions": r.get("Active_sessions"),
                "Dropped_packets": r.get("Dropped_packets"),
                "Top_blocked_IP": r.get("Top_blocked_IP"),
                "CPU_Usage": r.get("CPU_Usage"),
                "RAM_Usage": r.get("RAM_Usage"),
            })
    return registros


# ------------------------------- Cálculos ---------------------------------
def calcular_vazao(registros: list[dict]) -> int:
    """
    Para cada dispositivo, calcula o delta de bytes em relação à coleta anterior
    e a vazão em Mbps. Retorna quantos resets de contador foram detectados.
    """
    resets = 0
    por_dispositivo = {}
    for r in registros:
        por_dispositivo.setdefault(r["ID"], []).append(r)

    for lista in por_dispositivo.values():
        lista.sort(key=lambda x: x["ts"])
        anterior = None
        for atual in lista:
            atual.update(Delta_Sent=None, Delta_Recv=None, Intervalo_s=None,
                         Vazao_Sent_Mbps=None, Vazao_Recv_Mbps=None,
                         Vazao_Total_Mbps=None)
            if anterior is not None:
                intervalo = (atual["ts"] - anterior["ts"]).total_seconds()
                try:
                    d_sent = atual["Bytes_Sent"] - anterior["Bytes_Sent"]
                    d_recv = atual["Bytes_Recv"] - anterior["Bytes_Recv"]
                except TypeError:
                    d_sent = d_recv = None

                if d_sent is not None and intervalo > 0:
                    if d_sent < 0 or d_recv < 0:
                        resets += 1
                    else:
                        mbps_s = d_sent * 8 / intervalo / 1_000_000
                        mbps_r = d_recv * 8 / intervalo / 1_000_000
                        atual.update(
                            Delta_Sent=d_sent, Delta_Recv=d_recv,
                            Intervalo_s=round(intervalo, 1),
                            Vazao_Sent_Mbps=round(mbps_s, 4),
                            Vazao_Recv_Mbps=round(mbps_r, 4),
                            Vazao_Total_Mbps=round(mbps_s + mbps_r, 4),
                        )
            anterior = atual
    return resets


def definir_status_carga(r: dict) -> str:
    """Classifica a carga do dispositivo conforme os limites definidos."""
    status = []
    if (r.get("Active_conn") or 0) > LIMITE_ACTIVE_CONN:
        status.append("alta densidade")
    if (r.get("CPU_Usage") or 0) > LIMITE_CPU:
        status.append("gargalo de processamento")
    if (r.get("RAM_Usage") or 0) > LIMITE_RAM:
        status.append("OOM (Out Of Memory)")
    return " | ".join(status) if status else "normal"


def correlacionar_por_minuto(registros: list[dict]) -> dict:
    """
    Agrupa por minuto e compara o tráfego das APs (ponta) com o do firewall (centro).
    """
    minutos = {}
    for r in registros:
        r["Minuto"] = r["ts"].replace(second=0, microsecond=0)
        m = minutos.setdefault(r["Minuto"], {
            "conn_por_ap": {}, "sessoes": [],
            "soma_sent_aps": 0, "aps_com_delta": set(), "fw_delta_sent": None,
        })
        if r["Tipo"] == "AP":
            m["conn_por_ap"].setdefault(r["ID"], []).append(r.get("Active_conn") or 0)
            if r["Delta_Sent"] is not None:
                m["soma_sent_aps"] += r["Delta_Sent"]
                m["aps_com_delta"].add(r["ID"])
        else:
            if r.get("Active_sessions") is not None:
                m["sessoes"].append(r["Active_sessions"])
            if r["Delta_Sent"] is not None:
                # ACUMULA os deltas do minuto (antes só guardava o último)
                m["fw_delta_sent"] = (m["fw_delta_sent"] or 0) + r["Delta_Sent"]

    for m in minutos.values():
        m["soma_conn"] = round(sum(sum(v) / len(v) for v in m["conn_por_ap"].values()))
        m["fw_sessoes"] = round(sum(m["sessoes"]) / len(m["sessoes"])) if m["sessoes"] else None
        fw = m["fw_delta_sent"]
        if fw is None or len(m["aps_com_delta"]) < QTD_APS_ESPERADAS:
            m.update(diferenca=None, desvio=None, status="DADOS_INCOMPLETOS")
            continue
        diferenca = m["soma_sent_aps"] - fw
        if fw == 0:
            desvio = 0.0 if diferenca == 0 else 100.0
        else:
            desvio = abs(diferenca) / fw * 100
        m.update(
            diferenca=diferenca,
            desvio=round(desvio, 2),
            status="CONSISTENTE" if desvio <= TOLERANCIA_PCT else "DIVERGENTE",
        )
    return minutos


def montar_linha(r: dict, m: dict) -> dict:
    return {
        "Timestamp": r["ts"].strftime("%Y-%m-%d %H:%M:%S"),
        "Tipo": r["Tipo"], "ID": r["ID"],
        "Bytes_Sent": r["Bytes_Sent"], "Bytes_Recv": r["Bytes_Recv"],
        "Delta_Sent": r["Delta_Sent"], "Delta_Recv": r["Delta_Recv"],
        "Intervalo_s": r["Intervalo_s"],
        "Vazao_Sent_Mbps": r["Vazao_Sent_Mbps"],
        "Vazao_Recv_Mbps": r["Vazao_Recv_Mbps"],
        "Vazao_Total_Mbps": r["Vazao_Total_Mbps"],
        "Active_conn": r["Active_conn"], "Active_sessions": r["Active_sessions"],
        "Dropped_packets": r["Dropped_packets"], "Top_blocked_IP": r["Top_blocked_IP"],
        "CPU_Usage": r["CPU_Usage"], "RAM_Usage": r["RAM_Usage"],
        "status_carga": r["status_carga"],
        "Minuto_Soma_Conn_APs": m["soma_conn"],
        "Minuto_FW_Sessoes": m["fw_sessoes"],
        "Minuto_Soma_Delta_Sent_APs": m["soma_sent_aps"] if m["aps_com_delta"] else None,
        "Minuto_FW_Delta_Sent": m["fw_delta_sent"],
        "Minuto_Diferenca_Bytes": m["diferenca"],
        "Minuto_Desvio_pct": m["desvio"],
        "status_consistencia": m["status"],
    }


def gravar_csv(linhas: list[dict]) -> None:
    """
    Monta o CSV em memória e envia ao S3 (camada silver).
    O put_object é atômico: o objeto antigo só é substituído quando o novo
    upload termina, então quem lê nunca vê um arquivo pela metade.
    """
    buffer = io.StringIO()
    escritor = csv.DictWriter(buffer, fieldnames=COLUNAS)
    escritor.writeheader()
    escritor.writerows(linhas)

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=CHAVE_CSV,
        Body=buffer.getvalue().encode("utf-8-sig"),
        ContentType="text/csv",
    )


def consolidar() -> None:
    inicio = time.time()
    print("\n[CONSOLIDAÇÃO] Iniciando leitura dos dados brutos...")

    chaves = listar_chaves_bronze()
    chaves_aps = [c for c in chaves if PADRAO_ANTENAS.search(c)]
    chaves_fws = [c for c in chaves if PADRAO_FIREWALLS.search(c)]

    registros = ler_registros(chaves_aps, "AP") + ler_registros(chaves_fws, "FW")
    if not registros:
        print("[AVISO] Nenhum registro encontrado. Aguardando novas capturas...")
        return
    print(f"[LEITURA] {len(registros)} registro(s) válido(s) carregado(s).")

    print("[CÁLCULO] Calculando vazão (Mbps) entre coletas consecutivas...")
    resets = calcular_vazao(registros)
    if resets:
        print(f"[AVISO] {resets} reset(s) de contador detectado(s); vazão não calculada nesses pontos.")

    print("[CÁLCULO] Classificando status de carga...")
    for r in registros:
        r["status_carga"] = definir_status_carga(r)

    print("[CORRELAÇÃO] Comparando tráfego das APs com o firewall (por minuto)...")
    minutos = correlacionar_por_minuto(registros)

    registros.sort(key=lambda r: (r["ts"], r["Tipo"], r["ID"]))
    linhas = [montar_linha(r, minutos[r["Minuto"]]) for r in registros]
    gravar_csv(linhas)

    resumo = {}
    for m in minutos.values():
        resumo[m["status"]] = resumo.get(m["status"], 0) + 1
    alertas = sum(1 for r in registros if r["status_carga"] != "normal")
    print(f"[SALVO] {len(linhas)} linha(s) gravada(s) em 's3://{BUCKET_NAME}/{CHAVE_CSV}'.")
    print(f"[RESUMO] Consistência por minuto: {resumo} | Registros com alerta de carga: {alertas}")
    print(f"[OK] Consolidação concluída em {time.time() - inicio:.1f}s.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Consolida JSONs de APs e firewall (S3 bronze) em CSV (S3 silver).")
    parser.add_argument("--intervalo", type=int, default=INTERVALO_PADRAO_SEG,
                        help="segundos entre consolidações (padrão: 180)")
    parser.add_argument("--uma-vez", action="store_true",
                        help="executa apenas uma consolidação e encerra")
    args = parser.parse_args()

    print("=" * 60)
    print(" Consolidação de dados (Camada 02) iniciada")
    print(f" Origem: s3://{BUCKET_NAME}/{PREFIXO_BRONZE}/ | Saída: s3://{BUCKET_NAME}/{CHAVE_CSV}")
    if not args.uma_vez:
        print(f" Intervalo: {args.intervalo}s | Ctrl+C para encerrar.")
    print("=" * 60)

    try:
        while True:
            consolidar()
            if args.uma_vez:
                break
            print(f"[AGUARDANDO] Próxima consolidação em {args.intervalo} segundos...")
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\n[ENCERRADO] Consolidação interrompida pelo usuário. Até logo!")
    except Exception as erro:
        print(f"\n[ERRO] Falha inesperada: {erro}")


if __name__ == "__main__":
    main()
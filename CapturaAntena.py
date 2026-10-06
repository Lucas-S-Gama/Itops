"""
Captura de métricas de antenas (simuladas) - as 3 APs ao mesmo tempo.

A cada ciclo, o script coleta UMA vez as métricas reais da máquina (psutil) e
gera, a partir delas, os registros de ap01, ap02 e ap03 com leves divergências
entre si, simulando o monitoramento simultâneo das três antenas.

Coleta (por AP):
  - ID_antena        : ap01, ap02 e ap03 (todas no mesmo ciclo)
  - Bytes_Sent/Recv  : contadores de rede derivados de psutil.net_io_counters().
                       O tráfego real do ciclo é DIVIDIDO entre as APs (com
                       pesos levemente diferentes), então a soma das 3 APs
                       fecha com o tráfego real da máquina e os contadores
                       de cada AP continuam sempre crescentes.
  - Active_conn      : contagem SIMULADA de dispositivos (varia entre as APs)
  - CPU/RAM_Usage    : percentual real com pequena variação por AP

Armazenamento:
  - Pasta 'antenas/' (criada automaticamente se não existir)
  - Um arquivo JSON por dia PARA CADA antena, no formato:
        AAAA-MM-DD_HH-MM_apXX.json   (ex.: 2026-04-06_10-30_ap01.json)
    O HH-MM é o horário da primeira captura daquela antena no dia, quando o
    arquivo é criado. Nas capturas seguintes do mesmo dia, os registros são
    adicionados ao mesmo arquivo. Ao virar o dia, novos arquivos são criados.

Para encerrar, use Ctrl+C.
"""

import glob
import os
import random
import time
from datetime import datetime

import pandas as pd
import psutil

# ----------------------------- Configurações ------------------------------
ANTENAS = ["ap01", "ap02", "ap03"]
PASTA_SAIDA = "antenas"
INTERVALO_SEGUNDOS = 60  # tempo entre ciclos (use 60 para 1 em 1 minuto)

# Perfil de uso de cada AP: quanto maior, mais tráfego a antena recebe
PESOS_BASE = {"ap01": 1.0, "ap02": 0.9, "ap03": 1.1}
VARIACAO_PESO = 0.15   # +/-15% de oscilação do peso a cada ciclo
VARIACAO_CONN = 10     # +/- dispositivos conectados entre as APs
VARIACAO_PCT = 5.0     # +/- pontos percentuais em CPU e RAM entre as APs

# Estado entre ciclos (contadores acumulados simulados de cada AP)
_acumulado = {ap: {"sent": 0, "recv": 0} for ap in ANTENAS}
_ultimo_host = None


# ------------------------------- Coleta -----------------------------------
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
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # igual para as 3 APs

    # Tráfego real ocorrido desde o ciclo anterior (no 1º ciclo, o total atual)
    if _ultimo_host is None:
        delta_sent, delta_recv = rede.bytes_sent, rede.bytes_recv
    else:
        delta_sent = max(0, rede.bytes_sent - _ultimo_host[0])
        delta_recv = max(0, rede.bytes_recv - _ultimo_host[1])
    _ultimo_host = (rede.bytes_sent, rede.bytes_recv)

    fracoes = _sortear_fracoes()
    parte_sent = _dividir(delta_sent, fracoes)
    parte_recv = _dividir(delta_recv, fracoes)

    base_conn = random.randint(20, 100)  # carga geral do ciclo; cada AP varia em torno dela

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


# ----------------------------- Armazenamento ------------------------------
def obter_caminho_do_dia(id_antena: str) -> str:
    """
    Retorna o caminho do JSON do dia atual para a antena informada.
    Se já existir um arquivo de hoje para ela, reutiliza; senão, define um
    novo com o horário atual no nome (AAAA-MM-DD_HH-MM_apXX.json).
    """
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    agora = datetime.now()
    data_hoje = agora.strftime("%Y-%m-%d")

    existentes = sorted(
        glob.glob(os.path.join(PASTA_SAIDA, f"{data_hoje}_*_{id_antena}.json"))
    )
    if existentes:
        return existentes[0]

    nome = f"{data_hoje}_{agora.strftime('%H-%M')}_{id_antena}.json"
    return os.path.join(PASTA_SAIDA, nome)


def salvar_json(registro: dict) -> tuple[str, int]:
    """Adiciona o registro ao JSON do dia da antena. Retorna (caminho, total de linhas)."""
    caminho = obter_caminho_do_dia(registro["ID_antena"])
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
    print(" Monitoramento simultâneo de antenas iniciado")
    print(f" Antenas: {', '.join(ANTENAS)}")
    print(f" Intervalo: {INTERVALO_SEGUNDOS}s | Pasta: {PASTA_SAIDA}/")
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
                caminho, total = salvar_json(r)
                print(f"         salvo em '{caminho}' (total no dia: {total})")

            print(f"[TOTAL]  Soma de Bytes_Sent das APs: {soma_sent:,} B")

            # Desconta o tempo gasto na coleta para manter o intervalo definido
            espera = 60 - (time.time() % 60)
            print(f"[AGUARDANDO] Próximo ciclo em {espera:.0f} segundos...")
            time.sleep(espera)

    except KeyboardInterrupt:
        print("\n[ENCERRADO] Monitoramento interrompido pelo usuário. Até logo!")
    except Exception as erro:
        print(f"\n[ERRO] Falha inesperada: {erro}")


if __name__ == "__main__":
    main()
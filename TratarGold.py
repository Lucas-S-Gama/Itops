"""
Camada 03 - OURO (analytics): Mapa de Calor Logístico.

Pergunta de negócio: "Onde estão os usuários?"
Cruza o tráfego de cada antena (ponta) com a saída do firewall (centro) e
identifica qual antena/setor do prédio consome mais internet, para decidir
onde investir em links mais rápidos.

Lê o CSV consolidado (camada prata) e gera em 'ouro/':

  1. mapa_calor_logistico.csv  -> por hora e por antena: volume, vazão, % do tráfego
                                  das APs na hora, ranking e saída do firewall
  2. ranking_antenas.csv       -> visão geral do período: quem consome mais e
                                  quantas horas cada antena foi a líder

Uso:
    python gerar_ouro.py
    python gerar_ouro.py --entrada consolidado/dados_consolidados.csv --saida ouro
    python gerar_ouro.py --bucket meu-bucket --prefixo 03-gold   # envia ao S3 (opcional)
"""

import argparse
import os

import pandas as pd

ENTRADA_PADRAO = os.path.join("consolidado", "dados_consolidados.csv")
SAIDA_PADRAO = "ouro"

# Edite com os setores reais do prédio (cada antena cobre um setor)
SETORES = {"ap01": "Setor A", "ap02": "Setor B", "ap03": "Setor C"}


# ------------------------------- Leitura ----------------------------------
def carregar(caminho: str) -> pd.DataFrame:
    df = pd.read_csv(caminho, encoding="utf-8-sig")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["Hora"] = df["Timestamp"].dt.floor("h")
    # Volume total (enviado + recebido) de cada intervalo; vazio na 1ª coleta de cada dispositivo
    df["Bytes_Total"] = df["Delta_Sent"] + df["Delta_Recv"]
    return df


# ------------------------------ Agregações --------------------------------
def mapa_calor(df: pd.DataFrame) -> pd.DataFrame:
    """Uma linha por hora e antena, com a parcela de cada AP no tráfego da hora."""
    aps = df[df["Tipo"] == "AP"].dropna(subset=["Bytes_Total"])
    fw = df[df["Tipo"] == "FW"].dropna(subset=["Bytes_Total"])

    por_ap = aps.groupby(["Hora", "ID"]).agg(
        Bytes_Total=("Bytes_Total", "sum"),
        Vazao_Media_Mbps=("Vazao_Total_Mbps", "mean"),
        Vazao_Pico_Mbps=("Vazao_Total_Mbps", "max"),
        Amostras=("Timestamp", "count"),
    ).reset_index()

    total_aps_hora = por_ap.groupby("Hora")["Bytes_Total"].transform("sum")
    por_ap["Volume_MB"] = por_ap["Bytes_Total"] / 1_000_000
    por_ap["Pct_Trafego_APs"] = por_ap["Bytes_Total"] / total_aps_hora * 100
    por_ap["Ranking_na_Hora"] = (
        por_ap.groupby("Hora")["Bytes_Total"].rank(ascending=False, method="min").astype(int)
    )
    por_ap["Setor"] = por_ap["ID"].map(SETORES).fillna("N/D")

    # Saída do firewall na mesma hora (centro) e quanto das APs ela cobre
    saida_fw = fw.groupby("Hora")["Bytes_Total"].sum().rename("Bytes_Firewall")
    por_ap = por_ap.merge(saida_fw, on="Hora", how="left")
    por_ap["Firewall_MB"] = por_ap["Bytes_Firewall"] / 1_000_000
    por_ap["Soma_APs_MB"] = total_aps_hora / 1_000_000
    por_ap["Cobertura_APs_vs_FW_pct"] = total_aps_hora / por_ap["Bytes_Firewall"] * 100

    colunas = ["Hora", "ID", "Setor", "Volume_MB", "Pct_Trafego_APs", "Ranking_na_Hora",
               "Vazao_Media_Mbps", "Vazao_Pico_Mbps", "Soma_APs_MB", "Firewall_MB",
               "Cobertura_APs_vs_FW_pct", "Amostras"]
    resultado = por_ap[colunas].sort_values(["Hora", "Ranking_na_Hora"])
    return arredondar(resultado)


def ranking_antenas(mapa: pd.DataFrame) -> pd.DataFrame:
    """Visão geral do período: consumo total e quantas horas cada antena liderou."""
    g = mapa.groupby(["ID", "Setor"]).agg(
        Volume_Total_MB=("Volume_MB", "sum"),
        Vazao_Media_Mbps=("Vazao_Media_Mbps", "mean"),
        Vazao_Pico_Mbps=("Vazao_Pico_Mbps", "max"),
        Horas_Como_Lider=("Ranking_na_Hora", lambda s: (s == 1).sum()),
        Horas_Analisadas=("Hora", "count"),
    ).reset_index()
    g["Pct_do_Trafego_Total"] = g["Volume_Total_MB"] / g["Volume_Total_MB"].sum() * 100
    g = g.sort_values("Volume_Total_MB", ascending=False).reset_index(drop=True)
    g.insert(0, "Ranking", g.index + 1)
    return arredondar(g)


def arredondar(tabela: pd.DataFrame) -> pd.DataFrame:
    """Arredonda só as colunas numéricas de ponto flutuante."""
    num = tabela.select_dtypes("float").columns
    tabela[num] = tabela[num].round(4)
    return tabela


# ------------------------------ S3 (opcional) -----------------------------
def enviar_s3(arquivos: list[str], bucket: str, prefixo: str) -> None:
    import boto3  # importado aqui para não exigir boto3 quando não usar S3
    s3 = boto3.client("s3")
    for caminho in arquivos:
        chave = f"{prefixo}/{os.path.basename(caminho)}"
        s3.upload_file(caminho, bucket, chave)
        print(f"[S3] s3://{bucket}/{chave}")


# ------------------------------ Execução ----------------------------------
def main() -> None:
    p = argparse.ArgumentParser(description="Gera o Mapa de Calor Logístico (camada ouro).")
    p.add_argument("--entrada", default=ENTRADA_PADRAO)
    p.add_argument("--saida", default=SAIDA_PADRAO)
    p.add_argument("--bucket", help="se informado, envia os CSVs ao S3")
    p.add_argument("--prefixo", default="03-gold", help="prefixo no bucket (padrão: 03-gold)")
    args = p.parse_args()

    df = carregar(args.entrada)
    print(f"[LEITURA] {len(df)} linha(s) de '{args.entrada}'.")

    mapa = mapa_calor(df)
    ranking = ranking_antenas(mapa)

    os.makedirs(args.saida, exist_ok=True)
    caminhos = []
    for nome, tabela in {"mapa_calor_logistico.csv": mapa, "ranking_antenas.csv": ranking}.items():
        caminho = os.path.join(args.saida, nome)
        tabela.to_csv(caminho, index=False, encoding="utf-8-sig")
        caminhos.append(caminho)
        print(f"[OURO] {caminho} ({len(tabela)} linha(s))")

    lider = ranking.iloc[0]
    print(f"[RESULTADO] Maior consumo: {lider['ID']} ({lider['Setor']}) com "
          f"{lider['Pct_do_Trafego_Total']:.1f}% do tráfego das APs.")

    if args.bucket:
        enviar_s3(caminhos, args.bucket, args.prefixo)


if __name__ == "__main__":
    main()
import argparse
import io

import boto3
import pandas as pd

AWS_REGION = "us-east-1"
BUCKET_PADRAO = "itops-gama"
CHAVE_ENTRADA_PADRAO = "02-silver/dados_consolidados.csv"
PREFIXO_GOLD_PADRAO = "03-gold"

SETORES = {"ap01": "Setor A", "ap02": "Setor B", "ap03": "Setor C"}

s3_client = boto3.client("s3", region_name=AWS_REGION)

def carregar(bucket: str, chave: str) -> pd.DataFrame:
    """Baixa o CSV consolidado (prata) do S3 e prepara as colunas de análise."""
    resposta = s3_client.get_object(Bucket=bucket, Key=chave)
    df = pd.read_csv(io.BytesIO(resposta["Body"].read()), encoding="utf-8-sig")
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["Hora"] = df["Timestamp"].dt.floor("h")
    df["Bytes_Total"] = df["Delta_Sent"] + df["Delta_Recv"]
    return df

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


def enviar_csv_s3(tabela: pd.DataFrame, bucket: str, chave: str) -> None:
    """Converte a tabela em CSV (em memória) e envia ao S3."""
    corpo = tabela.to_csv(index=False)
    s3_client.put_object(
        Bucket=bucket,
        Key=chave,
        Body=corpo.encode("utf-8-sig"),
        ContentType="text/csv",
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Gera o Mapa de Calor Logístico (camada ouro) no S3.")
    p.add_argument("--bucket", default=BUCKET_PADRAO,
                   help=f"bucket S3 (padrão: {BUCKET_PADRAO})")
    p.add_argument("--chave-entrada", default=CHAVE_ENTRADA_PADRAO,
                   help=f"chave do CSV prata no bucket (padrão: {CHAVE_ENTRADA_PADRAO})")
    p.add_argument("--prefixo", default=PREFIXO_GOLD_PADRAO,
                   help=f"prefixo de saída no bucket (padrão: {PREFIXO_GOLD_PADRAO})")
    args = p.parse_args()

    df = carregar(args.bucket, args.chave_entrada)
    print(f"[LEITURA] {len(df)} linha(s) de 's3://{args.bucket}/{args.chave_entrada}'.")

    mapa = mapa_calor(df)
    ranking = ranking_antenas(mapa)

    for nome, tabela in {"mapa_calor_logistico.csv": mapa, "ranking_antenas.csv": ranking}.items():
        chave = f"{args.prefixo}/{nome}"
        enviar_csv_s3(tabela, args.bucket, chave)
        print(f"[OURO] s3://{args.bucket}/{chave} ({len(tabela)} linha(s))")

    lider = ranking.iloc[0]
    print(f"[RESULTADO] Maior consumo: {lider['ID']} ({lider['Setor']}) com "
          f"{lider['Pct_do_Trafego_Total']:.1f}% do tráfego das APs.")


if __name__ == "__main__":
    main()
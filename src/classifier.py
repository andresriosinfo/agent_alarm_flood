import pandas as pd
from src.config import FloodConfig
from src.flood_rules import severity_from_rate, is_flood_candidate, classify_flood_type, recommend_action


def classify_blocks(df_blocks: pd.DataFrame, baseline: dict, cfg: FloodConfig) -> pd.DataFrame:
    df = df_blocks.copy()

    if "prio1_share" not in df.columns:
        raise ValueError("El dataframe de bloques debe incluir la columna 'prio1_share'.")

    df["severity_v11"] = df["max_rate"].apply(lambda x: severity_from_rate(x, baseline))
    df["flood_candidate_v11"] = df.apply(lambda row: is_flood_candidate(row, baseline, cfg), axis=1)
    df["flood_type_v11"] = df.apply(lambda row: classify_flood_type(row, baseline, cfg), axis=1)
    df["recommended_action"] = df.apply(
        lambda row: recommend_action(row["flood_type_v11"], row["severity_v11"]),
        axis=1,
    )

    return df
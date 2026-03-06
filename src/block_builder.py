import pandas as pd
from src.config import FloodConfig


def build_alarm_blocks(
    df_alarms: pd.DataFrame,
    cfg: FloodConfig,
    tag_col: str = "TAGNAME",
    msg_col: str = "MESSAGE",
) -> pd.DataFrame:
    """
    Construye bloques de alarmas a partir de eventos crudos.

    Requiere:
    - columna de tiempo: cfg.time_col
    - columna de prioridad: cfg.priority_col
    - columna de tag
    - columna de mensaje
    """
    required_cols = [cfg.time_col, cfg.priority_col, tag_col, msg_col]
    missing = [c for c in required_cols if c not in df_alarms.columns]
    if missing:
        raise ValueError(f"Faltan columnas requeridas: {missing}")

    df = df_alarms.copy()
    df[cfg.time_col] = pd.to_datetime(df[cfg.time_col])
    df = df.sort_values(cfg.time_col).reset_index(drop=True)

    # Gap entre alarmas consecutivas
    df["gap_min"] = df[cfg.time_col].diff().dt.total_seconds().div(60)

    # Nuevo bloque si el gap supera el umbral
    df["new_block"] = df["gap_min"].isna() | (df["gap_min"] > cfg.block_gap_min)
    df["block_id"] = df["new_block"].cumsum()

    blocks = []

    for block_id, g in df.groupby("block_id", sort=True):
        g = g.sort_values(cfg.time_col)

        start = g[cfg.time_col].iloc[0]
        end = g[cfg.time_col].iloc[-1]
        n = len(g)

        duration_min = max((end - start).total_seconds() / 60.0, 1.0)

        per_min = g.set_index(cfg.time_col).resample("1min").size()
        max_rate = float(per_min.max()) if len(per_min) else float(n)

        unique_tags = int(g[tag_col].nunique())

        tag_counts = g[tag_col].value_counts(dropna=False)
        dominant_tag = tag_counts.index[0]
        dominant_tag_share = float(tag_counts.iloc[0] / n)

        msg_counts = g[msg_col].value_counts(dropna=False)
        dominant_msg = msg_counts.index[0]
        dominant_msg_share = float(msg_counts.iloc[0] / n)

        prio1_share = float((g[cfg.priority_col] == 1).mean())

        blocks.append(
            {
                "block_id": int(block_id),
                "start": start,
                "end": end,
                "duration_min": duration_min,
                "n": int(n),
                "max_rate": max_rate,
                "unique_tags": unique_tags,
                "dominant_tag": dominant_tag,
                "dominant_tag_share": dominant_tag_share,
                "dominant_msg": dominant_msg,
                "dominant_msg_share": dominant_msg_share,
                "prio1_share": prio1_share,
            }
        )

    df_blocks = pd.DataFrame(blocks)

    if df_blocks.empty:
        return df_blocks

    df_blocks = df_blocks[df_blocks["n"] >= cfg.min_block_size].reset_index(drop=True)
    return df_blocks
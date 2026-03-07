from __future__ import annotations

import pandas as pd

from src.config import DBConfig, DataSourceConfig, FloodConfig
from src.db import get_connection


def load_alarms(
    data_cfg: DataSourceConfig,
    db_cfg: DBConfig,
    flood_cfg: FloodConfig,
) -> pd.DataFrame:
    if data_cfg.mode.lower() == "csv":
        df = pd.read_csv(
            data_cfg.csv_path,
            parse_dates=[flood_cfg.time_col],
        )
        return df

    if data_cfg.mode.lower() == "sql":
        conn = get_connection(db_cfg)
        query = f"SELECT * FROM {db_cfg.schema}.{db_cfg.table}"
        df = pd.read_sql(query, conn)
        conn.close()

        if flood_cfg.time_col in df.columns:
            df[flood_cfg.time_col] = pd.to_datetime(df[flood_cfg.time_col], errors="coerce")

        return df

    raise ValueError(
        f"Modo de fuente de datos no soportado: {data_cfg.mode}. "
        "Usa 'csv' o 'sql'."
    )

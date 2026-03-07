from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd

from src.config import DBConfig, FloodConfig
from src.db import read_sql_df


ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(exist_ok=True)


def _compute_baseline_from_df(
    df_alarms: pd.DataFrame,
    flood_config: FloodConfig,
) -> dict:
    time_col = flood_config.time_col

    if df_alarms is None or df_alarms.empty:
        return {
            "scope": f"last_{flood_config.baseline_window_days}_days",
            "window_days": flood_config.baseline_window_days,
            "rate_p95": 0.0,
            "rate_p99": 0.0,
            "rate_p999": 0.0,
            "max_per_minute": 0,
            "n_minutes": 0,
        }

    df = df_alarms.copy()

    if time_col not in df.columns:
        raise ValueError(
            f"La columna de tiempo '{time_col}' no existe en el dataframe."
        )

    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col]).sort_values(time_col)

    if df.empty:
        return {
            "scope": f"last_{flood_config.baseline_window_days}_days",
            "window_days": flood_config.baseline_window_days,
            "rate_p95": 0.0,
            "rate_p99": 0.0,
            "rate_p999": 0.0,
            "max_per_minute": 0,
            "n_minutes": 0,
        }

    end_time = df[time_col].max()
    start_time = end_time - pd.Timedelta(days=flood_config.baseline_window_days)

    df_window = df[df[time_col] >= start_time].copy()

    if df_window.empty:
        df_window = df.copy()

    per_minute = (
        df_window.set_index(time_col)
        .resample("1min")
        .size()
        .rename("alarm_count")
        .to_frame()
    )

    if per_minute.empty:
        return {
            "scope": f"last_{flood_config.baseline_window_days}_days",
            "window_days": flood_config.baseline_window_days,
            "rate_p95": 0.0,
            "rate_p99": 0.0,
            "rate_p999": 0.0,
            "max_per_minute": 0,
            "n_minutes": 0,
        }

    counts = per_minute["alarm_count"]

    return {
        "scope": f"last_{flood_config.baseline_window_days}_days",
        "window_days": flood_config.baseline_window_days,
        "rate_p95": float(counts.quantile(0.95)),
        "rate_p99": float(counts.quantile(0.99)),
        "rate_p999": float(counts.quantile(0.999)),
        "max_per_minute": int(counts.max()),
        "n_minutes": int(len(counts)),
    }


def _load_df_from_sql(
    conn,
    db_config: DBConfig,
    flood_config: FloodConfig,
) -> pd.DataFrame:
    query = f"SELECT * FROM {db_config.schema}.{db_config.table}"
    df = read_sql_df(conn, query)

    if flood_config.time_col in df.columns:
        df[flood_config.time_col] = pd.to_datetime(
            df[flood_config.time_col],
            errors="coerce",
        )

    return df


def get_or_create_baseline(
    *,
    df_alarms: Optional[pd.DataFrame] = None,
    conn=None,
    db_config: Optional[DBConfig] = None,
    flood_config: Optional[FloodConfig] = None,
    force_recompute: bool = False,
) -> dict:
    """
    Soporta dos modos:
    1) Nuevo: get_or_create_baseline(df_alarms=df, flood_config=...)
    2) Viejo: get_or_create_baseline(conn=..., db_config=..., flood_config=...)
    """
    flood_config = flood_config or FloodConfig()

    cache_file = ARTIFACTS_DIR / "baseline_cache.csv"

    if not force_recompute and cache_file.exists():
        try:
            cached = pd.read_csv(cache_file)
            if not cached.empty:
                return cached.iloc[0].to_dict()
        except Exception:
            pass

    if df_alarms is None:
        if conn is None or db_config is None:
            raise ValueError(
                "Debes proporcionar df_alarms o bien conn + db_config."
            )
        df_alarms = _load_df_from_sql(conn, db_config, flood_config)

    baseline = _compute_baseline_from_df(df_alarms, flood_config)

    pd.DataFrame([baseline]).to_csv(cache_file, index=False)

    return baseline

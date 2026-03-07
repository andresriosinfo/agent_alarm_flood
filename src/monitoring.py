import pandas as pd

from src.db import read_sql_df
from src.config import DBConfig, FloodConfig


def read_recent_alarm_events(
    conn,
    db_config: DBConfig,
    flood_config: FloodConfig,
    minutes: int = 15,
    anchor_time: str | None = None,
) -> pd.DataFrame:
    """
    Lee alarmas de los últimos N minutos.

    Si anchor_time es None:
        usa el timestamp máximo disponible en la tabla.
    Si anchor_time viene definido:
        usa ese timestamp como final de la ventana.
    """
    if anchor_time is None:
        q = f"""
        WITH mx AS (
            SELECT MAX([{flood_config.time_col}]) AS anchor_t
            FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}]
        )
        SELECT
            [{flood_config.time_col}] AS event_time,
            [TAG_DESCRIPTION] AS tag,
            [ALARM_DESCRIPTION] AS message,
            [{flood_config.priority_col}] AS priority,
            [ALARM_ID] AS alarm_id,
            [LOCATION] AS location,
            [GRP] AS grp
        FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}]
        CROSS JOIN mx
        WHERE [{flood_config.time_col}] >= DATEADD(minute, -{minutes}, mx.anchor_t)
          AND [{flood_config.time_col}] <= mx.anchor_t
          AND [{flood_config.time_col}] IS NOT NULL
        ORDER BY [{flood_config.time_col}] ASC
        """
        df = read_sql_df(conn, q)
    else:
        q = f"""
        SELECT
            [{flood_config.time_col}] AS event_time,
            [TAG_DESCRIPTION] AS tag,
            [ALARM_DESCRIPTION] AS message,
            [{flood_config.priority_col}] AS priority,
            [ALARM_ID] AS alarm_id,
            [LOCATION] AS location,
            [GRP] AS grp
        FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}]
        WHERE [{flood_config.time_col}] >= DATEADD(minute, -{minutes}, CAST(? AS DATETIME))
          AND [{flood_config.time_col}] <= CAST(? AS DATETIME)
          AND [{flood_config.time_col}] IS NOT NULL
        ORDER BY [{flood_config.time_col}] ASC
        """
        df = read_sql_df(conn, q, params=[anchor_time, anchor_time])

    if df.empty:
        return df

    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")
    df = df.dropna(subset=["event_time"]).sort_values("event_time").reset_index(drop=True)
    return df


def _slice_last_minutes(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    tmax = df["event_time"].max()
    tmin = tmax - pd.Timedelta(minutes=minutes)
    return df[df["event_time"] >= tmin].copy()


def _alarm_rate_per_min(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)

    s = (
        df.set_index("event_time")
        .resample("1min")
        .size()
        .astype(float)
    )
    return s


def _safe_mean(series: pd.Series) -> float:
    if series is None or len(series) == 0:
        return 0.0
    return float(series.mean())


def _prio1_share(df: pd.DataFrame) -> float:
    if df.empty or "priority" not in df.columns:
        return 0.0
    return float((df["priority"] == 1).mean())


def _unique_tags(df: pd.DataFrame) -> int:
    if df.empty or "tag" not in df.columns:
        return 0
    return int(df["tag"].nunique())


def _new_tags_last_1m_vs_prev(df_5m: pd.DataFrame) -> int:
    if df_5m.empty:
        return 0

    tmax = df_5m["event_time"].max()
    last_1m_start = tmax - pd.Timedelta(minutes=1)
    prev_4m_start = tmax - pd.Timedelta(minutes=5)

    df_last_1m = df_5m[df_5m["event_time"] >= last_1m_start]
    df_prev_4m = df_5m[
        (df_5m["event_time"] >= prev_4m_start) &
        (df_5m["event_time"] < last_1m_start)
    ]

    tags_last = set(df_last_1m["tag"].dropna().astype(str).unique())
    tags_prev = set(df_prev_4m["tag"].dropna().astype(str).unique())

    return int(len(tags_last - tags_prev))


def compute_recent_features(df_recent: pd.DataFrame, baseline: dict) -> dict:
    if df_recent.empty:
        return {
            "window_end": None,
            "n_recent": 0,
            "rate_1m": 0.0,
            "rate_5m_avg": 0.0,
            "rate_15m_avg": 0.0,
            "rate_growth_1m_vs_5m": 0.0,
            "prio1_share_5m": 0.0,
            "unique_tags_5m": 0,
            "new_tags_1m": 0,
            "rate_vs_p95": 0.0,
            "rate_vs_p99": 0.0,
        }

    df_recent = df_recent.copy()
    df_recent["event_time"] = pd.to_datetime(df_recent["event_time"], errors="coerce")
    df_recent = df_recent.dropna(subset=["event_time"]).sort_values("event_time")

    df_1m = _slice_last_minutes(df_recent, 1)
    df_5m = _slice_last_minutes(df_recent, 5)
    df_15m = _slice_last_minutes(df_recent, 15)

    rate_1m = float(len(df_1m))

    per_min_5m = _alarm_rate_per_min(df_5m)
    per_min_15m = _alarm_rate_per_min(df_15m)

    rate_5m_avg = _safe_mean(per_min_5m)
    rate_15m_avg = _safe_mean(per_min_15m)

    eps = 1e-6
    rate_growth = float((rate_1m + eps) / (rate_5m_avg + eps))

    prio1_share_5m = _prio1_share(df_5m)
    unique_tags_5m = _unique_tags(df_5m)
    new_tags_1m = _new_tags_last_1m_vs_prev(df_5m)

    rate_vs_p95 = float(rate_1m / (baseline["rate_p95"] + eps))
    rate_vs_p99 = float(rate_1m / (baseline["rate_p99"] + eps))

    return {
        "window_end": df_recent["event_time"].max(),
        "n_recent": int(len(df_recent)),
        "rate_1m": rate_1m,
        "rate_5m_avg": rate_5m_avg,
        "rate_15m_avg": rate_15m_avg,
        "rate_growth_1m_vs_5m": rate_growth,
        "prio1_share_5m": prio1_share_5m,
        "unique_tags_5m": unique_tags_5m,
        "new_tags_1m": new_tags_1m,
        "rate_vs_p95": rate_vs_p95,
        "rate_vs_p99": rate_vs_p99,
    }
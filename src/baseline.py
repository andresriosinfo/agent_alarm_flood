import numpy as np
from src.config import DBConfig, FloodConfig
from src.db import read_sql_df


def compute_baseline_all_history(conn, db_config: DBConfig, flood_config: FloodConfig) -> dict:
    q = f"""
    WITH minute_counts AS (
      SELECT
        DATEADD(minute, DATEDIFF(minute, 0, [{flood_config.time_col}]), 0) AS [minute],
        COUNT(*) AS alarms
      FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}]
      GROUP BY DATEADD(minute, DATEDIFF(minute, 0, [{flood_config.time_col}]), 0)
    )
    SELECT alarms
    FROM minute_counts;
    """

    df = read_sql_df(conn, q)
    x = df["alarms"].to_numpy()

    return {
        "scope": "all_history",
        "rate_p95": float(np.quantile(x, 0.95)),
        "rate_p99": float(np.quantile(x, 0.99)),
        "rate_p999": float(np.quantile(x, 0.999)),
        "max_per_minute": int(x.max()),
        "n_minutes": int(len(x)),
    }


def compute_baseline_last_days(conn, db_config: DBConfig, flood_config: FloodConfig) -> dict:
    days = flood_config.baseline_window_days

    q = f"""
    WITH mx AS (
      SELECT MAX([{flood_config.time_col}]) AS mx_t
      FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}]
    ),
    minute_counts AS (
      SELECT
        DATEADD(minute, DATEDIFF(minute, 0, a.[{flood_config.time_col}]), 0) AS [minute],
        COUNT(*) AS alarms
      FROM [{db_config.database}].[{db_config.schema}].[{db_config.table}] a
      CROSS JOIN mx
      WHERE a.[{flood_config.time_col}] >= DATEADD(day, -{days}, mx.mx_t)
      GROUP BY DATEADD(minute, DATEDIFF(minute, 0, a.[{flood_config.time_col}]), 0)
    )
    SELECT alarms
    FROM minute_counts;
    """

    df = read_sql_df(conn, q)
    x = df["alarms"].to_numpy()

    return {
        "scope": f"last_{days}_days",
        "window_days": days,
        "rate_p95": float(np.quantile(x, 0.95)),
        "rate_p99": float(np.quantile(x, 0.99)),
        "rate_p999": float(np.quantile(x, 0.999)),
        "max_per_minute": int(x.max()),
        "n_minutes": int(len(x)),
    }
from src.db import read_sql_df
from src.config import DBConfig, FloodConfig


def read_alarm_events(conn, db_config: DBConfig, flood_config: FloodConfig):
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
    WHERE [{flood_config.time_col}] IS NOT NULL
    ORDER BY [{flood_config.time_col}] ASC
    """
    return read_sql_df(conn, q)
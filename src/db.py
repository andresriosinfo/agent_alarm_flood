import pyodbc
import pandas as pd
from src.config import DBConfig, get_db_password


def get_connection(db_config: DBConfig) -> pyodbc.Connection:
    password = get_db_password(db_config.password_env_var)

    conn_str = (
        f"DRIVER={db_config.driver};"
        f"SERVER={db_config.server},{db_config.port};"
        f"DATABASE={db_config.database};"
        f"UID={db_config.username};"
        f"PWD={password};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def read_sql_df(conn: pyodbc.Connection, query: str, params=None) -> pd.DataFrame:
    return pd.read_sql(query, conn, params=params)
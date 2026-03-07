from dataclasses import dataclass
import os


@dataclass(frozen=True)
class DBConfig:
    server: str = "10.147.17.185"
    port: int = 1433
    username: str = "cmpcuser"
    password_env_var: str = "OTMS_SQL_PASSWORD_DEV"
    driver: str = "{ODBC Driver 17 for SQL Server}"
    database: str = "cmpc_20240925_093000"
    schema: str = "dbo"
    table: str = "ypf_alarms"


@dataclass(frozen=True)
class FloodConfig:
    time_col: str = "ALARMDATETIME"
    priority_col: str = "PRIORITY"

    block_gap_min: int = 5
    min_block_size: int = 50

    dom_msg_th: float = 0.70
    dom_tag_th: float = 0.85
    prio1_th: float = 0.80

    infra_tags_th: int = 200
    subsystem_tags_th: int = 50
    chatter_tag_th: float = 0.95

    baseline_window_days: int = 30


def get_db_password(env_var: str) -> str:
    password = S4nT4f3_+*4Xl
    if not password:
        raise RuntimeError(
            f"No se encontró la variable de entorno {env_var}. "
            f"Defínela antes de ejecutar el agente."
        )
    return password

from __future__ import annotations

from typing import Optional

import pandas as pd

from src.config import DBConfig, FloodConfig
from src.db import read_sql_df


def _load_df_from_sql(conn, db_config: DBConfig, flood_config: FloodConfig) -> pd.DataFrame:
    query = f"SELECT * FROM {db_config.schema}.{db_config.table}"
    df = read_sql_df(conn, query)

    if flood_config.time_col in df.columns:
        df[flood_config.time_col] = pd.to_datetime(
            df[flood_config.time_col],
            errors="coerce",
        )

    return df


def _find_tag_column(df: pd.DataFrame) -> Optional[str]:
    candidates = [
        "TAG",
        "TAGNAME",
        "TAG_NAME",
        "POINTNAME",
        "POINT_NAME",
        "SOURCE",
        "NAME",
        "DESCRIPTION",
        "MESSAGE",
        "MSG",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None


def _normalize_priority_series(df: pd.DataFrame, priority_col: str) -> pd.Series:
    if priority_col not in df.columns:
        return pd.Series([0] * len(df), index=df.index)

    s = df[priority_col].astype(str).str.strip().str.upper()

    mapped = s.map(
        {
            "1": 1,
            "P1": 1,
            "PRIORITY_1": 1,
            "HIGH": 1,
            "2": 2,
            "P2": 2,
            "3": 3,
            "P3": 3,
            "4": 4,
            "P4": 4,
        }
    )

    numeric = pd.to_numeric(s, errors="coerce")
    return mapped.fillna(numeric).fillna(0)


def _safe_ratio(num: float, den: float) -> float:
    if den is None or den == 0:
        return 0.0
    return float(num) / float(den)


def _compute_recent_features(
    df_alarms: pd.DataFrame,
    flood_config: FloodConfig,
    anchor_dt: pd.Timestamp,
    baseline: dict,
) -> dict:
    time_col = flood_config.time_col
    priority_col = flood_config.priority_col

    df = df_alarms.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    df = df[df[time_col] <= anchor_dt].sort_values(time_col)

    if df.empty:
        return {
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

    win_1m_start = anchor_dt - pd.Timedelta(minutes=1)
    win_5m_start = anchor_dt - pd.Timedelta(minutes=5)
    win_15m_start = anchor_dt - pd.Timedelta(minutes=15)

    df_1m = df[df[time_col] > win_1m_start]
    df_5m = df[df[time_col] > win_5m_start]
    df_15m = df[df[time_col] > win_15m_start]

    rate_1m = float(len(df_1m))
    rate_5m_avg = float(len(df_5m)) / 5.0
    rate_15m_avg = float(len(df_15m)) / 15.0

    prio_num = _normalize_priority_series(df_5m, priority_col)
    prio1_share_5m = float((prio_num == 1).mean()) if len(df_5m) > 0 else 0.0

    tag_col = _find_tag_column(df)
    if tag_col:
        tags_5m = set(df_5m[tag_col].astype(str))
        tags_prev_4m = set(
            df[
                (df[time_col] > anchor_dt - pd.Timedelta(minutes=5))
                & (df[time_col] <= anchor_dt - pd.Timedelta(minutes=1))
            ][tag_col].astype(str)
        )
        tags_1m = set(df_1m[tag_col].astype(str))

        unique_tags_5m = len(tags_5m)
        new_tags_1m = len(tags_1m - tags_prev_4m)
    else:
        unique_tags_5m = 0
        new_tags_1m = 0

    rate_growth_1m_vs_5m = _safe_ratio(rate_1m, max(rate_5m_avg, 1e-6))

    rate_p95 = float(baseline.get("rate_p95", 0.0) or 0.0)
    rate_p99 = float(baseline.get("rate_p99", 0.0) or 0.0)

    return {
        "rate_1m": rate_1m,
        "rate_5m_avg": rate_5m_avg,
        "rate_15m_avg": rate_15m_avg,
        "rate_growth_1m_vs_5m": rate_growth_1m_vs_5m,
        "prio1_share_5m": prio1_share_5m,
        "unique_tags_5m": int(unique_tags_5m),
        "new_tags_1m": int(new_tags_1m),
        "rate_vs_p95": _safe_ratio(rate_1m, max(rate_p95, 1e-6)),
        "rate_vs_p99": _safe_ratio(rate_1m, max(rate_p99, 1e-6)),
    }


def _compute_risk_score(features: dict) -> int:
    score = 0.0

    score += min(features.get("rate_vs_p95", 0.0), 2.0) * 20
    score += min(features.get("rate_vs_p99", 0.0), 2.0) * 25

    growth = features.get("rate_growth_1m_vs_5m", 0.0)
    if growth >= 2.0:
        score += 15
    elif growth >= 1.5:
        score += 10
    elif growth >= 1.2:
        score += 5

    prio1 = features.get("prio1_share_5m", 0.0)
    if prio1 >= 0.6:
        score += 15
    elif prio1 >= 0.35:
        score += 8

    unique_tags = features.get("unique_tags_5m", 0)
    if unique_tags >= 20:
        score += 15
    elif unique_tags >= 8:
        score += 8

    new_tags = features.get("new_tags_1m", 0)
    if new_tags >= 10:
        score += 10
    elif new_tags >= 4:
        score += 5

    return int(max(0, min(round(score), 100)))


def _state_from_score(score: int) -> str:
    if score >= 75:
        return "FLOOD DETECTED"
    if score >= 55:
        return "HIGH RISK OF FLOOD"
    if score >= 30:
        return "ELEVATED RISK"
    return "NORMAL"


def _posture_from_state(state: str) -> str:
    if state == "FLOOD DETECTED":
        return "Escalate and prioritize alarm flood handling."
    if state == "HIGH RISK OF FLOOD":
        return "Increase monitoring attention and prepare flood response."
    if state == "ELEVATED RISK":
        return "Maintain enhanced monitoring and review the affected area."
    return "Continue normal monitoring."


def _action_from_state(state: str) -> str:
    if state == "FLOOD DETECTED":
        return "auto_incident"
    if state == "HIGH RISK OF FLOOD":
        return "group_and_prioritize"
    if state == "ELEVATED RISK":
        return "notify_and_prioritize"
    return "no_action"


def _event_type_from_features(features: dict) -> str:
    unique_tags = features.get("unique_tags_5m", 0)
    prio1 = features.get("prio1_share_5m", 0.0)
    growth = features.get("rate_growth_1m_vs_5m", 0.0)

    if unique_tags >= 20:
        return "INFRASTRUCTURE_EVENT"
    if growth >= 1.8 and prio1 >= 0.35:
        return "SUBSYSTEM_TRIP_EVENT"
    if unique_tags <= 3 and growth >= 1.3:
        return "LOCAL_PROCESS_INSTABILITY"
    return "OTHER_OR_NO_FLOOD"


def _severity_from_state(state: str, score: int) -> str:
    if state == "FLOOD DETECTED":
        return "severe"
    if state in {"HIGH RISK OF FLOOD", "ELEVATED RISK"} or score >= 40:
        return "medium"
    return "none"


def _build_current_event(
    df_alarms: pd.DataFrame,
    flood_config: FloodConfig,
    anchor_dt: pd.Timestamp,
    state: str,
    risk_score: int,
    features: dict,
) -> Optional[dict]:
    if state != "FLOOD DETECTED":
        return None

    time_col = flood_config.time_col
    tag_col = _find_tag_column(df_alarms)

    df = df_alarms.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    df = df[df[time_col] <= anchor_dt].sort_values(time_col)

    window_start = anchor_dt - pd.Timedelta(minutes=15)
    event_df = df[df[time_col] >= window_start].copy()

    if event_df.empty:
        return None

    per_min = (
        event_df.set_index(time_col)
        .resample("1min")
        .size()
        .rename("alarm_count")
    )

    event_type = _event_type_from_features(features)

    unique_tags = int(event_df[tag_col].astype(str).nunique()) if tag_col else 0

    return {
        "start": str(event_df[time_col].min()),
        "end": str(event_df[time_col].max()),
        "duration_min": round(
            (event_df[time_col].max() - event_df[time_col].min()).total_seconds() / 60.0,
            2,
        ),
        "n": int(len(event_df)),
        "unique_tags": unique_tags,
        "max_rate": int(per_min.max()) if not per_min.empty else 0,
        "flood_type_v11": event_type,
        "severity_v11": _severity_from_state(state, risk_score),
        "recommended_action": _action_from_state(state),
    }


def assess_current_state(
    *,
    df_alarms: Optional[pd.DataFrame] = None,
    conn=None,
    db_config: Optional[DBConfig] = None,
    flood_config: Optional[FloodConfig] = None,
    anchor_time: str,
    baseline: dict,
) -> dict:
    """
    Soporta dos modos:
    1) Nuevo: assess_current_state(df_alarms=df, flood_config=..., anchor_time=..., baseline=...)
    2) Viejo: assess_current_state(conn=..., db_config=..., flood_config=..., anchor_time=..., baseline=...)
    """
    flood_config = flood_config or FloodConfig()

    if df_alarms is None:
        if conn is None or db_config is None:
            raise ValueError(
                "Debes proporcionar df_alarms o bien conn + db_config."
            )
        df_alarms = _load_df_from_sql(conn, db_config, flood_config)

    time_col = flood_config.time_col

    if time_col not in df_alarms.columns:
        raise ValueError(
            f"La columna de tiempo '{time_col}' no existe en el dataframe."
        )

    anchor_dt = pd.to_datetime(anchor_time)

    df = df_alarms.copy()
    df[time_col] = pd.to_datetime(df[time_col], errors="coerce")
    df = df.dropna(subset=[time_col])
    df = df[df[time_col] <= anchor_dt].sort_values(time_col)

    features = _compute_recent_features(df, flood_config, anchor_dt, baseline)
    risk_score = _compute_risk_score(features)
    state = _state_from_score(risk_score)
    posture = _posture_from_state(state)
    action = _action_from_state(state)
    current_event = _build_current_event(
        df_alarms=df,
        flood_config=flood_config,
        anchor_dt=anchor_dt,
        state=state,
        risk_score=risk_score,
        features=features,
    )

    rate_vs_p95 = features.get("rate_vs_p95", 0.0)
    growth = features.get("rate_growth_1m_vs_5m", 0.0)
    regime_change = bool(rate_vs_p95 >= 1.0 or growth >= 1.4)

    return {
        "anchor_time": str(anchor_dt),
        "current_state": state,
        "risk_score": int(risk_score),
        "regime_change": regime_change,
        "operational_posture": posture,
        "recommended_action": action,
        "flood_detected": state == "FLOOD DETECTED",
        "recent_features": features,
        "current_event": current_event,
    }

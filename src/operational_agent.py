from src.baseline import compute_baseline_last_days
from src.monitoring import read_recent_alarm_events, compute_recent_features
from src.risk_engine import (
    detect_regime_change,
    compute_risk_score,
    get_operational_state,
    get_operational_posture,
)
from src.block_builder import build_alarm_blocks
from src.classifier import classify_blocks


def _prepare_recent_for_block_builder(df_recent, flood_config):
    if df_recent.empty:
        return df_recent.copy()

    df = df_recent.copy()
    df[flood_config.time_col] = df["event_time"]
    df["TAGNAME"] = df["tag"]
    df["MESSAGE"] = df["message"]
    df[flood_config.priority_col] = df["priority"]
    return df


def _detect_active_flood_from_recent(df_recent, baseline: dict, flood_config) -> tuple[bool, dict | None]:
    if df_recent.empty:
        return False, None

    df_prepared = _prepare_recent_for_block_builder(df_recent, flood_config)

    try:
        df_blocks = build_alarm_blocks(
            df_prepared,
            flood_config,
            tag_col="TAGNAME",
            msg_col="MESSAGE",
        )
    except Exception:
        return False, None

    if df_blocks.empty:
        return False, None

    df_classified = classify_blocks(df_blocks, baseline, flood_config)

    candidates = df_classified[
        (df_classified["flood_candidate_v11"] == True) &
        (df_classified["flood_type_v11"] != "OTHER_OR_NO_FLOOD")
    ].copy()

    if candidates.empty:
        return False, None

    latest = candidates.sort_values("start").iloc[-1].to_dict()
    return True, latest


def assess_current_state(
    conn,
    db_config,
    flood_config,
    anchor_time: str | None = None,
    baseline: dict | None = None,
) -> dict:
    """
    Evaluación operacional unificada del estado actual o de un punto histórico.
    Si anchor_time viene definido, evalúa la ventana que termina en ese instante.

    Si baseline viene definido, lo reutiliza.
    Si no viene, lo calcula.
    """
    if baseline is None:
        baseline = compute_baseline_last_days(conn, db_config, flood_config)

    df_recent = read_recent_alarm_events(
        conn,
        db_config,
        flood_config,
        minutes=15,
        anchor_time=anchor_time,
    )

    features = compute_recent_features(df_recent, baseline)
    regime_change = detect_regime_change(features, baseline)
    risk_score, reasons = compute_risk_score(features, baseline)

    flood_detected, current_event = _detect_active_flood_from_recent(
        df_recent,
        baseline,
        flood_config,
    )

    state = get_operational_state(
        risk_score=risk_score,
        flood_detected=flood_detected,
    )

    posture = get_operational_posture(state)

    if regime_change and not reasons:
        reasons = ["recent alarm behavior differs from the normal baseline"]

    return {
        "anchor_time": anchor_time,
        "baseline": baseline,
        "recent_features": features,
        "regime_change": regime_change,
        "risk_score": risk_score,
        "current_state": state,
        "operational_posture": posture,
        "reasons": reasons,
        "flood_detected": flood_detected,
        "current_event": current_event,
    }
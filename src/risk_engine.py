def detect_regime_change(features: dict, baseline: dict) -> bool:
    """
    Cambio de régimen simple y explicable.
    """
    return bool(
        features["rate_1m"] >= baseline["rate_p95"]
        or features["rate_5m_avg"] >= baseline["rate_p95"]
        or features["rate_growth_1m_vs_5m"] >= 1.5
        or features["prio1_share_5m"] >= 0.60
        or features["unique_tags_5m"] >= 20
    )


def compute_risk_score(features: dict, baseline: dict) -> tuple[int, list[str]]:
    """
    Score heurístico de riesgo de flood.
    Devuelve:
    - score entre 0 y 100
    - reasons: lista de razones activadas
    """
    score = 0
    reasons = []

    if features["rate_1m"] >= baseline["rate_p95"]:
        score += 20
        reasons.append("alarm rate in the last minute exceeded historical p95")

    if features["rate_1m"] >= baseline["rate_p99"]:
        score += 20
        reasons.append("alarm rate in the last minute exceeded historical p99")

    if features["rate_5m_avg"] >= baseline["rate_p95"]:
        score += 10
        reasons.append("5-minute average alarm rate is above historical p95")

    if features["rate_growth_1m_vs_5m"] >= 1.5:
        score += 15
        reasons.append("alarm activity is accelerating relative to the recent 5-minute window")

    if features["prio1_share_5m"] >= 0.60:
        score += 10
        reasons.append("priority 1 concentration increased in the recent 5-minute window")

    if features["unique_tags_5m"] >= 20:
        score += 10
        reasons.append("affected tag diversity increased in the recent 5-minute window")

    if features["new_tags_1m"] >= 10:
        score += 15
        reasons.append("many new tags appeared in the last minute")

    score = min(score, 100)
    return int(score), reasons


def get_operational_state(
    risk_score: int,
    flood_detected: bool,
) -> str:
    if flood_detected:
        return "FLOOD DETECTED"
    if risk_score >= 70:
        return "HIGH RISK OF FLOOD"
    if risk_score >= 40:
        return "ELEVATED RISK"
    return "NORMAL"


def get_operational_posture(state: str) -> str:
    if state == "FLOOD DETECTED":
        return "Escalate and prioritize alarm flood handling."
    if state == "HIGH RISK OF FLOOD":
        return "Increase monitoring attention and prepare flood response."
    if state == "ELEVATED RISK":
        return "Maintain enhanced monitoring and review the affected area."
    return "Continue normal monitoring."
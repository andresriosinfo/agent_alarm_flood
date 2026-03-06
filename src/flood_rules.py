from src.config import FloodConfig


def severity_from_rate(max_rate: float, baseline: dict) -> str:
    if max_rate >= baseline["rate_p999"]:
        return "severe"
    if max_rate >= baseline["rate_p99"]:
        return "medium"
    return "none"


def is_flood_candidate(row, baseline: dict, cfg: FloodConfig) -> bool:
    return (
        row["n"] >= cfg.min_block_size
        and row["max_rate"] >= baseline["rate_p99"]
        and row["prio1_share"] >= cfg.prio1_th
        and (
            row["dominant_msg_share"] >= cfg.dom_msg_th
            or row["dominant_tag_share"] >= cfg.dom_tag_th
        )
    )


def classify_flood_type(row, baseline: dict, cfg: FloodConfig) -> str:
    if not row["flood_candidate_v11"]:
        return "OTHER_OR_NO_FLOOD"

    ut = row["unique_tags"]
    dts = row["dominant_tag_share"]
    dms = row["dominant_msg_share"]
    sev = row["severity_v11"]
    pr1 = row["prio1_share"]

    if dms >= cfg.dom_msg_th and ut >= cfg.infra_tags_th and sev in {"medium", "severe"}:
        return "INFRASTRUCTURE_EVENT"

    if dts >= cfg.chatter_tag_th and sev in {"medium", "severe"}:
        return "CHATTERING_POINT"

    if ut <= 15 and dms >= cfg.dom_msg_th and dts < cfg.dom_tag_th and sev in {"medium", "severe"}:
        return "LOCAL_PROCESS_INSTABILITY"

    if ut >= cfg.subsystem_tags_th and dms >= 0.50 and pr1 >= cfg.prio1_th and sev in {"medium", "severe"}:
        return "SUBSYSTEM_TRIP_EVENT"

    return "OTHER_OR_NO_FLOOD"


def recommend_action(flood_type: str, severity: str) -> str:
    if flood_type == "INFRASTRUCTURE_EVENT":
        return "auto_incident" if severity == "severe" else "notify_and_prioritize"

    if flood_type == "CHATTERING_POINT":
        return "group_and_prioritize"

    if flood_type == "LOCAL_PROCESS_INSTABILITY":
        return "notify_and_prioritize"

    if flood_type == "SUBSYSTEM_TRIP_EVENT":
        return "auto_incident" if severity == "severe" else "notify_and_prioritize"

    return "no_action"
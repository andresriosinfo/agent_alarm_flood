from pathlib import Path
import pandas as pd

ARTIFACTS_DIR = Path("artifacts")
RESULTS_FILE = ARTIFACTS_DIR / "flood_events_classified.csv"


def load_results() -> pd.DataFrame:
    if not RESULTS_FILE.exists():
        return pd.DataFrame()

    df = pd.read_csv(RESULTS_FILE)

    for col in ["start", "end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


def get_relevant_events(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

    if "flood_candidate_v11" not in df.columns or "flood_type_v11" not in df.columns:
        return df.copy()

    out = df[
        (df["flood_candidate_v11"] == True) &
        (df["flood_type_v11"] != "OTHER_OR_NO_FLOOD")
    ].copy()

    if "start" in out.columns:
        out = out.sort_values("start", ascending=False)

    return out


def get_event_label(row) -> str:
    start = row.get("start", "N/A")
    ev_type = row.get("flood_type_v11", "UNKNOWN")
    sev = row.get("severity_v11", "N/A")
    n = row.get("n", "N/A")
    return f"{start} | {ev_type} | {sev} | alarms={n}"


def build_operator_message(row) -> str:
    event_type = str(row.get("flood_type_v11", "UNKNOWN"))
    severity = str(row.get("severity_v11", "none"))
    action = str(row.get("recommended_action", "no_action"))

    duration = row.get("duration_min", "N/A")
    n = row.get("n", "N/A")
    unique_tags = row.get("unique_tags", "N/A")

    type_map = {
        "SUBSYSTEM_TRIP_EVENT": "The pattern is consistent with a subsystem trip affecting multiple related alarms.",
        "CHATTERING_POINT": "The pattern is consistent with a chattering point repeatedly generating alarms.",
        "LOCAL_PROCESS_INSTABILITY": "The pattern suggests local process instability concentrated in a limited area.",
        "INFRASTRUCTURE_EVENT": "The pattern suggests a broad infrastructure-related event affecting many tags.",
        "OTHER_OR_NO_FLOOD": "No clear operational flood pattern was identified."
    }

    action_map = {
        "auto_incident": "Open or escalate an operational incident and prioritize response.",
        "notify_and_prioritize": "Notify operations and prioritize review of the affected area.",
        "group_and_prioritize": "Group repeated alarms and prioritize inspection of the source point.",
        "no_action": "No immediate action recommended."
    }

    base_text = type_map.get(event_type, "Operational pattern detected.")
    action_text = action_map.get(action, "Review event.")

    return (
        f"{base_text} "
        f"The event lasted {round(float(duration), 2) if duration != 'N/A' else duration} minutes, "
        f"generated {int(n) if n != 'N/A' else n} alarms, "
        f"and affected {int(unique_tags) if unique_tags != 'N/A' else unique_tags} tags. "
        f"Severity was classified as {severity}. "
        f"Recommended action: {action_text}"
    )


def get_status_text(df_relevant: pd.DataFrame) -> str:
    if df_relevant.empty:
        return "NORMAL"
    return "FLOOD DETECTED"
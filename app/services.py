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


def get_latest_event(df: pd.DataFrame):
    if df.empty:
        return None
    return df.sort_values("start").iloc[-1]


def get_summary_metrics(df: pd.DataFrame) -> dict:
    if df.empty:
        return {
            "total_events": 0,
            "latest_severity": "N/A",
            "latest_type": "N/A",
            "latest_action": "N/A",
        }

    latest = get_latest_event(df)

    return {
        "total_events": int(len(df)),
        "latest_severity": latest.get("severity_v11", "N/A"),
        "latest_type": latest.get("flood_type_v11", "N/A"),
        "latest_action": latest.get("recommended_action", "N/A"),
    }
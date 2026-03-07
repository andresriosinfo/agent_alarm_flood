import json
from pathlib import Path

from src.baseline import compute_baseline_last_days


ARTIFACTS_DIR = Path("artifacts")
BASELINE_FILE = ARTIFACTS_DIR / "baseline_last_days.json"


def save_baseline(baseline: dict, filepath: Path = BASELINE_FILE) -> None:
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(baseline, f, indent=2, ensure_ascii=False)


def load_baseline(filepath: Path = BASELINE_FILE) -> dict | None:
    if not filepath.exists():
        return None

    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def get_or_create_baseline(conn, db_config, flood_config, force_recompute: bool = False) -> dict:
    """
    Si existe baseline guardado y no se fuerza recálculo, lo reutiliza.
    Si no existe, lo calcula, lo guarda y lo retorna.
    """
    if not force_recompute:
        baseline = load_baseline()
        if baseline is not None:
            return baseline

    baseline = compute_baseline_last_days(conn, db_config, flood_config)
    save_baseline(baseline)
    return baseline
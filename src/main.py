from pathlib import Path
import pandas as pd

from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.baseline import compute_baseline_all_history, compute_baseline_last_days
from src.classifier import classify_blocks


def main():
    db_config = DBConfig()
    flood_config = FloodConfig()

    # =========================
    # Config de ejecución
    # =========================
    USE_FAST_BASELINE = True   # True = usa últimos N días; False = usa todo el histórico

    conn = get_connection(db_config)

    # =========================
    # Baseline
    # =========================
    if USE_FAST_BASELINE:
        baseline = compute_baseline_last_days(conn, db_config, flood_config)
    else:
        baseline = compute_baseline_all_history(conn, db_config, flood_config)

    print("Baseline:", baseline)

    # =========================
    # Cargar bloques precomputados
    # =========================
    project_root = Path(__file__).resolve().parent.parent
    input_path = project_root / "artifacts" / "blocks_features_v11_with_prio1.csv"
    output_path = project_root / "artifacts" / "flood_events_classified.csv"

    print(f"Leyendo archivo: {input_path}")

    if not input_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo esperado: {input_path}")

    df_blocks = pd.read_csv(input_path, parse_dates=["start", "end"])

    # =========================
    # Clasificación
    # =========================
    df_result = classify_blocks(df_blocks, baseline, flood_config)

    print("\nConteo por tipo:")
    print(df_result["flood_type_v11"].value_counts())

    # =========================
    # Guardado
    # =========================
    df_result.to_csv(output_path, index=False)
    print(f"\nResultado guardado en: {output_path}")

    conn.close()


if __name__ == "__main__":
    main()
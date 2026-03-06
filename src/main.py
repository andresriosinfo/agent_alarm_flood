import pandas as pd
from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.baseline import compute_baseline_all_history
from src.classifier import classify_blocks


def main():
    db_config = DBConfig()
    flood_config = FloodConfig()

    conn = get_connection(db_config)

    baseline = compute_baseline_all_history(conn, db_config, flood_config)
    print("Baseline:", baseline)

    df_blocks = pd.read_csv("artifacts/blocks_features_v11_with_prio1.csv", parse_dates=["start", "end"])
    df_result = classify_blocks(df_blocks, baseline, flood_config)

    print(df_result["flood_type_v11"].value_counts())
    df_result.to_csv("artifacts/flood_events_classified.csv", index=False)

    conn.close()


if __name__ == "__main__":
    main()
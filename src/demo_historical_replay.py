import argparse
from pprint import pprint

from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.baseline import compute_baseline_last_days
from src.operational_agent import assess_current_state


def main():
    parser = argparse.ArgumentParser(
        description="Replay histórico del agente operacional en una fecha/hora específica."
    )
    parser.add_argument(
        "--anchor-time",
        required=True,
        help='Fecha/hora final de la ventana, formato: "YYYY-MM-DD HH:MM:SS"',
    )
    args = parser.parse_args()

    db_config = DBConfig()
    flood_config = FloodConfig()

    conn = get_connection(db_config)

    try:
        print("\nCalculating baseline once...")
        baseline = compute_baseline_last_days(conn, db_config, flood_config)
        print("Baseline ready.\n")

        result = assess_current_state(
            conn=conn,
            db_config=db_config,
            flood_config=flood_config,
            anchor_time=args.anchor_time,
            baseline=baseline,
        )

        print("\n================ HISTORICAL REPLAY ASSESSMENT ================\n")
        print("Anchor time:", result["anchor_time"])
        print("Current state:", result["current_state"])
        print("Risk score:", result["risk_score"])
        print("Regime change:", result["regime_change"])
        print("Operational posture:", result["operational_posture"])

        print("\nReasons:")
        if result["reasons"]:
            for r in result["reasons"]:
                print("-", r)
        else:
            print("- no significant early warning signals")

        print("\nRecent features:")
        pprint(result["recent_features"])

        print("\nFlood detected:", result["flood_detected"])
        if result["current_event"] is not None:
            print("\nCurrent classified event:")
            pprint(result["current_event"])

        print("\n=============================================================\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
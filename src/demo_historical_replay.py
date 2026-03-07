import argparse
from pprint import pprint

from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.baseline_cache import get_or_create_baseline
from src.operational_agent import assess_current_state


def print_result(result: dict):
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


def main():
    parser = argparse.ArgumentParser(
        description="Replay histórico del agente operacional para una o varias fechas."
    )
    parser.add_argument(
        "--anchor-time",
        action="append",
        required=True,
        help='Fecha/hora final de la ventana, formato: "YYYY-MM-DD HH:MM:SS". '
             "Puedes pasar este argumento varias veces.",
    )
    parser.add_argument(
        "--force-recompute-baseline",
        action="store_true",
        help="Fuerza el recálculo del baseline aunque exista uno guardado en artifacts.",
    )
    args = parser.parse_args()

    db_config = DBConfig()
    flood_config = FloodConfig()

    conn = get_connection(db_config)

    try:
        print("\nLoading cached baseline or computing it once...")
        baseline = get_or_create_baseline(
            conn=conn,
            db_config=db_config,
            flood_config=flood_config,
            force_recompute=args.force_recompute_baseline,
        )
        print("Baseline ready.")

        for anchor_time in args.anchor_time:
            print(f"\nRunning replay for: {anchor_time}")
            result = assess_current_state(
                conn=conn,
                db_config=db_config,
                flood_config=flood_config,
                anchor_time=anchor_time,
                baseline=baseline,
            )

            if result is None:
                print(f"\nERROR: assess_current_state devolvió None para anchor_time={anchor_time}\n")
                continue

            print_result(result)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
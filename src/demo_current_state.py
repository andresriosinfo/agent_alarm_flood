from pprint import pprint

from src.config import DBConfig, FloodConfig
from src.db import get_connection
from src.operational_agent import assess_current_state


def main():
    db_config = DBConfig()
    flood_config = FloodConfig()

    conn = get_connection(db_config)

    try:
        result = assess_current_state(conn, db_config, flood_config)

        print("\n================ CURRENT OPERATIONAL ASSESSMENT ================\n")
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

        print("\n===============================================================\n")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
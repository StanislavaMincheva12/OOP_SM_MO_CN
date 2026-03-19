"""
Dynamic MICROALERTS updates from alerts_df.
Connects log.txt pipeline → DB table.
"""

import pandas as pd
from .db_loader import get_db_connection 

def create_microalerts_table(db_path: str):
    """Recreate table matching alerts_df (first run only)."""
    with get_db_connection(db_path) as conn:
        cur = conn.cursor()
        # Split into TWO executes
        cur.execute("DROP TABLE IF EXISTS MICROALERTS")
        cur.execute("""
            CREATE TABLE MICROALERTS (
                ALERT_ID INTEGER,
                WARD_ID INTEGER,
                ORG_ID INTEGER,
                ORG_NAME TEXT,
                NUM_PATIENTS INTEGER,
                START_TIME TEXT,
                SEVERITY INTEGER,
                THRESHOLD INTEGER,
                WINDOW_DAYS INTEGER,
                PRIMARY KEY (WARD_ID, ORG_ID, START_TIME)
            )
        """)
        conn.commit()
    print("✅ MICROALERTS table recreated")


def update_microalerts(alerts_df: pd.DataFrame, db_path: str) -> int:
    if alerts_df.empty:
        print("No alerts to update.")
        return 0
    
    with get_db_connection(db_path) as conn:  
        alerts_df.to_sql("MICROALERTS", conn, if_exists="append", index=False)
    
    print(f"✅ Updated {len(alerts_df)} alerts in MICROALERTS")
    return len(alerts_df)


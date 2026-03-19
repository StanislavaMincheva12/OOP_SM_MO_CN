"""This is the main file that runs: connect and load, repo, alerts, update, log txt, visualizations."""


import sys
import pandas as pd
from datetime import datetime

from simulation.alerts_builders import generate_episodes, generate_alerts
from database.db_connector_loader import TableLoader
from database.data_repository import DataRepository
from database.data_preparation import prepare_microbiology_data
from database.data_update import create_microalerts_table, update_microalerts
from microbiology.pathogens import load_default_pathogens
from visualization.visualization import AlertsLoader, AlertsDashboard

DB_PATH = "/Volumes/T7/OOP_SM_MO_CN-final_branch/Final_ver_9_march/OOP_database.db"

TABLES = [
    "PATIENTS", "CAREGIVERS", "D_ITEMS", "ADMISSIONS", "ICUSTAYS",
    "NOTEEVENTS", "MICROBIOLOGYEVENTS", "TRANSFERS", "OUTPUTEVENTS", "CHARTEVENTS"
]


def main() -> pd.DataFrame:
    # Step 1: Load data via TableLoader class
    loader = TableLoader(DB_PATH)
    data = loader.load_tables(TABLES)

    # Step 2: Run pipeline
    repo     = DataRepository.from_dict(data)
    registry = load_default_pathogens()
    ward_pos_all = prepare_microbiology_data(repo)
    episodes_df  = generate_episodes(ward_pos_all, registry)
    alerts       = generate_alerts(episodes_df, registry)

    # Step 3: Build alerts DataFrame
    alerts_df = pd.DataFrame([
        {
            "ALERT_ID":     alert.id,
            "WARD_ID":      alert.ward_id,
            "ORG_ID":       getattr(alert, "org_id", 0),
            "ORG_NAME":     alert.pathogen.key,
            "NUM_PATIENTS": alert.curr_patient_number,
            "START_TIME":   alert.start_time,
            "SEVERITY":     int(alert.pathogen.danger_weight * 10),
            "THRESHOLD":    alert.pathogen.get_ward_threshold(alert.ward_size),
            "WINDOW_DAYS":  alert.pathogen.time_window_days
        }
        for alert in alerts
    ]).sort_values(by="START_TIME")

    # Step 4: Append to log.txt with timestamp header
    run_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open("log.txt", "a") as f:
        f.write(f"\n{'='*60}\n")
        f.write(f"RUN: {run_timestamp} | Alerts: {len(alerts_df)}\n")
        f.write(f"{'='*60}\n")
        f.write(alerts_df.to_csv(sep="|", index=False))

    print("\nAlerts table:")
    print(alerts_df.to_string(index=False))
    print(f"\nGenerated {len(alerts_df)} alerts. Appended to log.txt.")

    return alerts_df


if __name__ == "__main__":
    # Step 1: Pipeline → log.txt
    alerts_df = main()

    # Step 2: Update DB
    print("\nUpdating DB...")
    create_microalerts_table(DB_PATH)
    inserted = update_microalerts(alerts_df, DB_PATH)
    print(f"✅ DB updated ({inserted} rows)!")

    # Step 3: Visualizations from DB
    print("\nGenerating visualizations...")
    AlertsDashboard(
        AlertsLoader(DB_PATH).load().data
    ).build_all()

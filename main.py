"""This is the main file that runs: connect and load, repo, alerts, update, log txt, visualizations."""


# Import from modules
from database.data_update import create_microalerts_table, update_microalerts

from visualization.visualization import AlertsLoader, AlertsDashboard

from pipeline.hospital_alert_system import HospitalAlertSystem

from utils.logger import AlertsLogger
import pandas as pd


# Stating the global parameteres
DB_PATH = "OOP_database.db"

TABLES = [
    "PATIENTS", "CAREGIVERS", "D_ITEMS", "ADMISSIONS", "ICUSTAYS",
    "NOTEEVENTS", "MICROBIOLOGYEVENTS", "TRANSFERS", "OUTPUTEVENTS", "CHARTEVENTS"
]


# Main run
if __name__ == "__main__":
    system = HospitalAlertSystem(DB_PATH, TABLES)
    alerts = system.run()
    alerts_df = pd.DataFrame([alert.to_dict() for alert in alerts])

    logger = AlertsLogger()
    logger.log(alerts_df)

    print("\nUpdating DB...")
    create_microalerts_table(DB_PATH)
    inserted = update_microalerts(alerts_df, DB_PATH)
    print(f"✅ DB updated ({inserted} rows)!")

    # Step 3: Visualizations from DB
    print("\nGenerating visualizations...")
    AlertsDashboard(
        AlertsLoader(DB_PATH).load().data
    ).build_all()



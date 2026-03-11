import sys
import pandas as pd

from .alerts_builders import generate_episodes, generate_alerts
from ..database.db_loader import load_tables
from ..database.data_repository import DataRepository
from ..database.data_preparation import prepare_microbiology_data
from ..microbiology.pathogens import load_default_pathogens


def main():

    db_path = "Final_ver_9_march/OOP_database.db"

    tables = [
        "PATIENTS",
        "CAREGIVERS",
        "D_ITEMS",
        "ADMISSIONS",
        "ICUSTAYS",
        "NOTEEVENTS",
        "MICROBIOLOGYEVENTS",
        "TRANSFERS",
        "OUTPUTEVENTS",
        "CHARTEVENTS",
    ]

    # Load data
    data = load_tables(db_path, tables)
    repo = DataRepository.from_dict(data)

    # Load pathogen registry
    registry = load_default_pathogens()

    # Prepare microbiology data
    ward_pos_all = prepare_microbiology_data(repo)

    # Generate episodes
    episodes_df = generate_episodes(ward_pos_all, registry)

    # Generate alerts
    alerts = generate_alerts(episodes_df, registry)

    alerts_data = []

    for alert in alerts:
        alerts_data.append({
            "ALERT_ID": alert.id,
            "WARD_ID": alert.ward_id,
            "ORG_ID": alert.org_id,
            "ORG_NAME": alert.pathogen.key,
            "NUM_PATIENTS": alert.curr_patient_number,
            "START_TIME": alert.start_time,
            "SEVERITY": int(alert.pathogen.danger_weight * 10),
            "THRESHOLD": alert.pathogen.get_ward_threshold(alert.ward_size),
            "WINDOW_DAYS": alert.pathogen.time_window_days
        })

    alerts_df = pd.DataFrame(alerts_data).sort_values(by="START_TIME")

    print("\nAlerts table:")
    print(alerts_df.to_string(index=False))


if __name__ == "__main__":

    with open("log.txt", "w") as log_file:
        sys.stdout = log_file
        main()

    sys.stdout = sys.__stdout__
    print("Output written to log.txt")
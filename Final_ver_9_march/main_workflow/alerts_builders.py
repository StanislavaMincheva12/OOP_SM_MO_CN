import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from microbiology.pathogens import PathogenRegistry
from microbiology.alerts import MicrobiologyAlert

def generate_episodes(ward_pos_all, registry: PathogenRegistry):
    """
    Generate episodes from processed ward data using the pathogen registry.
    Returns episodes_df DataFrame.
    """
    episodes_all = []

    for org_name, pathogen in registry._by_key.items():  # iterate registry
        ward_data = ward_pos_all[ward_pos_all["ORG_NAME"] == org_name].copy()
        if ward_data.empty:
            continue

        # Group by WARD_ID first (multi-ward support)
        ward_episodes = []
        for ward_id, ward_group in ward_data.groupby("WARD_ID"):
            df = ward_group.sort_values("CHARTDATE").reset_index(drop=True)
            ward_size = ward_group["WARD_SIZE"].iloc[0]

            # Episode logic (same as yours, but per-ward)
            episode_ids = []
            episode_id = 0
            episode_start = None
            episode_end = None

            for _, row in df.iterrows():
                if episode_start is None:
                    episode_start = row["CHARTDATE"]
                    episode_end = row["OUTTIME"]
                    episode_ids.append(episode_id)
                else:
                    still_occupied = pd.notna(episode_end) and pd.notna(row["CHARTDATE"]) and row["CHARTDATE"] <= episode_end
                    gap = (row["CHARTDATE"] - episode_start).total_seconds() / 86400 if pd.notna(row["CHARTDATE"]) and pd.notna(episode_start) else float('inf')

                    if still_occupied or gap <= pathogen.time_window_days:
                        # Extend episode
                        if pd.notna(row["OUTTIME"]):
                            if pd.notna(episode_end):
                                episode_end = max(episode_end, row["OUTTIME"]) # type: ignore
                            else:
                                episode_end = row["OUTTIME"]
                        # New episode
                        episode_id += 1
                        episode_start = row["CHARTDATE"]
                        episode_end = row["OUTTIME"]

                    episode_ids.append(episode_id)

            df["EPISODE_ID"] = episode_ids

            # Aggregate episodes
            ep = df.groupby("EPISODE_ID").agg(
                start_time=("CHARTDATE", "min"),
                end_time=("OUTTIME", "max"),
                culture_events=("ROW_ID", "count"),
                unique_patients=("SUBJECT_ID", "nunique"),
            ).reset_index(drop=True)

            ep["ward_id"] = ward_id
            ep["ward_size"] = ward_size
            ep["org_name"] = pathogen.key
            ep["danger_weight"] = pathogen.danger_weight
            ep["window_days"] = pathogen.time_window_days
            ep["threshold"] = pathogen.get_ward_threshold(ward_size)  # ← dynamic per ward size!
            ep["alert"] = ep["unique_patients"] >= ep["threshold"]

            ward_episodes.append(ep)

        # Combine ward episodes for this pathogen
        if ward_episodes:
            pathogen_ep = pd.concat(ward_episodes, ignore_index=True)
            episodes_all.append(pathogen_ep)

    episodes_df = pd.concat(episodes_all, ignore_index=True) if episodes_all else pd.DataFrame()

    print(f"Generated {len(episodes_df)} episodes across {episodes_df['ward_id'].nunique()} wards")
    print(episodes_df.groupby("ward_id")["alert"].sum().sort_values(ascending=False))

    return episodes_df

def generate_alerts(episodes_df, registry: PathogenRegistry):
    """
    Generate Alert objects from episodes DataFrame.
    Returns list of Alert instances.
    """
    alerts = []
    counter_id = 0

    for _, episode in episodes_df.iterrows():
        if episode["alert"]:
            pathogen = registry.get(episode["org_name"])
            if pathogen:
                alert = MicrobiologyAlert(
                    counter_id=counter_id,
                    pathogen=pathogen,
                    ward_id=int(episode["ward_id"]),
                    ward_size=int(episode["ward_size"]),
                    start_time=episode["start_time"],
                    alert_type="WARD",
                    curr_patient_number=int(episode["unique_patients"])
                )
                alerts.append(alert)
                counter_id += 1

    return alerts

if __name__ == "__main__":
    # Redirect output to log.txt
    with open('log.txt', 'w') as log_file:
        sys.stdout = log_file

        from database.db_loader import load_tables
        from database.data_repository import DataRepository
        from main_workflow.data_preparation import prepare_microbiology_data
        from microbiology.pathogens import load_default_pathogens

        db_path = "../OOP_database.db"

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

        print('Patients table:', repo.patients)

        # Output organism names with ORG_ITEMID from data
        organism_df = repo.microbiologyevents[['ORG_NAME', 'ORG_ITEMID']].drop_duplicates().dropna()
        organism_items = list(zip(organism_df['ORG_NAME'], organism_df['ORG_ITEMID']))
        print("Organism names with ORG_ITEMID:")
        for org_name, org_id in organism_items[:10]:  # Sample first 10
            print(f"('{org_name}', {org_id})")

        # Load pathogen registry
        registry = load_default_pathogens()

        # Prepare data
        ward_pos_all = prepare_microbiology_data(repo)

        # Generate episodes
        episodes_df = generate_episodes(ward_pos_all, registry)

        # Generate alerts
        alerts = generate_alerts(episodes_df, registry)

        # Create alerts DataFrame
        alerts_data = []
        for alert in alerts:
            alerts_data.append({
                "ALERT_ID": alert.id,
                "WARD_ID": alert.ward_id,
                "ORG_ID": alert.org_id,
                "ORG_NAME": alert.pathogen.key,
                "NUM_PATIENTS": alert.curr_patient_number,
                "CULTURE_EVENTS": episodes_df.loc[episodes_df["alert"] & (episodes_df["ward_id"] == alert.ward_id) & (episodes_df["org_name"] == alert.pathogen.key), "culture_events"].iloc[0] if len(episodes_df.loc[episodes_df["alert"] & (episodes_df["ward_id"] == alert.ward_id) & (episodes_df["org_name"] == alert.pathogen.key)]) > 0 else 0,
                "START_TIME": alert.start_time,
                "END_TIME": episodes_df.loc[episodes_df["alert"] & (episodes_df["ward_id"] == alert.ward_id) & (episodes_df["org_name"] == alert.pathogen.key), "end_time"].iloc[0] if len(episodes_df.loc[episodes_df["alert"] & (episodes_df["ward_id"] == alert.ward_id) & (episodes_df["org_name"] == alert.pathogen.key)]) > 0 else None,
                "SEVERITY": int(alert.pathogen.danger_weight * 10),
                "THRESHOLD": alert.pathogen.get_ward_threshold(alert.ward_size),
                "WINDOW_DAYS": alert.pathogen.time_window_days
            })

        alerts_df = pd.DataFrame(alerts_data)
        print("\nAlerts table head:")
        print(alerts_df.head().to_string(index=False))

    # Restore stdout
    sys.stdout = sys.__stdout__
    print("Output written to log.txt")

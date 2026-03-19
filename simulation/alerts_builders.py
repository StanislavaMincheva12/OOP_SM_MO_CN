import pandas as pd
from microbiology.pathogens import PathogenRegistry
from microbiology.alerts import MicrobiologyAlert
from database.db_loader import get_db_connection

def generate_episodes(ward_pos_all, registry: PathogenRegistry):
    episodes_all = []

    for org_name, pathogen in registry._by_key.items():
        ward_data = ward_pos_all[ward_pos_all["ORG_NAME"] == org_name].copy()
        if ward_data.empty:
            continue

        ward_episodes = []
        for ward_id, ward_group in ward_data.groupby("WARD_ID"):
            df = ward_group.sort_values("CHARTDATE").reset_index(drop=True)
            ward_size = ward_group["WARD_SIZE"].iloc[0]

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
                        if pd.notna(row["OUTTIME"]):
                            if pd.notna(episode_end):
                                episode_end = max(episode_end, row["OUTTIME"])
                            else:
                                episode_end = row["OUTTIME"]
                    else:
                        episode_id += 1
                        episode_start = row["CHARTDATE"]
                        episode_end = row["OUTTIME"]

                    episode_ids.append(episode_id)

            df["EPISODE_ID"] = episode_ids

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
            ep["threshold"] = pathogen.get_ward_threshold(ward_size)
            ep["alert"] = ep["unique_patients"] >= ep["threshold"]

            ward_episodes.append(ep)

        if ward_episodes:
            pathogen_ep = pd.concat(ward_episodes, ignore_index=True)
            episodes_all.append(pathogen_ep)

    episodes_df = pd.concat(episodes_all, ignore_index=True) if episodes_all else pd.DataFrame()
    print(f"Generated {len(episodes_df)} episodes across {episodes_df['ward_id'].nunique() if not episodes_df.empty else 0} wards")
    return episodes_df

def generate_alerts(episodes_df, registry: PathogenRegistry):
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

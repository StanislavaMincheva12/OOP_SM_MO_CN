import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pandas as pd
from datetime import timedelta


def prepare_microbiology_data(data_repo):
    """
    Prepare microbiology data for alert generation.
    Returns processed ward_pos_all DataFrame.
    """
    microbiologyevents = data_repo.microbiologyevents.copy()
    icustays = data_repo.icustays.copy()

    microbiologyevents.columns = microbiologyevents.columns.str.upper()
    icustays.columns = icustays.columns.str.upper()

    ALL_WARD_IDS = [52, 12, 50, 57, 7, 23, 15, 33, 14]

    # WARD SIZES TB FIXED
    WARD_SIZES = {
        52: 10,
        12: 20, 
        50: 10,
        57: 5,   
        7:  20,
        23: 15, 
        15: 10,
        33: 20,
        14: 10,
    }

    # Pre-compute HADM_IDs per ward
    ward_hadm = {}
    for ward_id in ALL_WARD_IDS:
        ward_hadm[ward_id] = icustays.loc[
            (icustays["FIRST_WARDID"] == ward_id) | (icustays["LAST_WARDID"] == ward_id),
            "HADM_ID"
        ].dropna().unique()

    microbiologyevents["CHARTDATE"] = pd.to_datetime(microbiologyevents["CHARTDATE"], errors="coerce")

    # Process ALL wards
    all_ward_pos = []
    for ward_id in ALL_WARD_IDS:
        print(f"Processing ward {ward_id} (size: {WARD_SIZES[ward_id]})")

        ward_pos = microbiologyevents[
            microbiologyevents["HADM_ID"].isin(ward_hadm[ward_id])
            & microbiologyevents["ORG_NAME"].notna()
            & microbiologyevents["CHARTDATE"].notna()
        ].copy()

        if len(ward_pos) == 0:
            print(f"  No microbiology data for ward {ward_id}")
            continue

        ward_pos["ORG_NAME"] = ward_pos["ORG_NAME"].astype(str).str.upper().str.strip()
        ward_pos["WARD_ID"] = ward_id
        ward_pos["WARD_SIZE"] = WARD_SIZES[ward_id]
        ward_pos = ward_pos.sort_values(["ORG_NAME", "CHARTDATE"])

        all_ward_pos.append(ward_pos)
        print(f"  Found {len(ward_pos)} positive cultures")

    # Combine all wards
    ward_pos_all = pd.concat(all_ward_pos, ignore_index=True) if all_ward_pos else pd.DataFrame()

    # Discharge times for ALL wards with length of stay column (LOS)
    ward_discharges = {}
    for ward_id in ALL_WARD_IDS:
        ward_discharges[ward_id] = icustays[
            (icustays["FIRST_WARDID"] == ward_id) | (icustays["LAST_WARDID"] == ward_id)
        ][["SUBJECT_ID", "OUTTIME", "LOS"]].copy()
        ward_discharges[ward_id]["OUTTIME"] = pd.to_datetime(ward_discharges[ward_id]["OUTTIME"])
        ward_discharges[ward_id]["LOS"] = ward_discharges[ward_id]["LOS"].astype('float')
    
    # Join discharge times onto positive cultures (per-ward merge)
    ward_pos_list = []
    for ward_id in ALL_WARD_IDS:
        ward_pos = ward_pos_all[ward_pos_all["WARD_ID"] == ward_id].copy()
        if len(ward_pos) > 0:
            ward_pos = ward_pos.merge(
                ward_discharges[ward_id],
                on="SUBJECT_ID",
                how="left",
                suffixes=("", f"_ward{ward_id}")
            )
            ward_pos_list.append(ward_pos)
    ward_pos_all = pd.concat(ward_pos_list, ignore_index=True) if ward_pos_list else pd.DataFrame()

    # Ensure datetime consistency
    ward_pos_all["OUTTIME"] = pd.to_datetime(ward_pos_all["OUTTIME"], errors="coerce")
    ward_pos_all["CHARTDATE"] = pd.to_datetime(ward_pos_all["CHARTDATE"], errors="coerce")
    
    start_date = pd.to_datetime("2026-03-11")

    # Get unique patients
    patients = (
        ward_pos_all[["SUBJECT_ID", "LOS"]] # add Length of Stay to record accurate stay
        .drop_duplicates()
        .sort_values("SUBJECT_ID")
        .reset_index(drop=True)
    )

    # Generate sequential CHARTDATE
    patients["CHARTDATE"] = [
        start_date + timedelta(days=i)
        for i in range(len(patients))
    ]



    # OUTTIME = CHARTTIME + Length of stay of a patient in ward
    patients["OUTTIME"] = patients["CHARTDATE"] + pd.to_timedelta(patients["LOS"], unit="D")

    # Merge back to main table
    ward_pos_all = ward_pos_all.merge(
        patients[["SUBJECT_ID", "CHARTDATE", "OUTTIME"]],
        on="SUBJECT_ID",
        how="left",
        suffixes=("", "_synthetic")
    )

    # Replace old values
    ward_pos_all["CHARTDATE"] = ward_pos_all["CHARTDATE_synthetic"]
    ward_pos_all["OUTTIME"] = ward_pos_all["OUTTIME_synthetic"]

    # Cleanup
    ward_pos_all = ward_pos_all.drop(columns=["CHARTDATE_synthetic", "OUTTIME_synthetic"])

    return ward_pos_all

"""
Simple test - load MIMIC data into dataclasses
"""

import pandas as pd
from DataClasses import Patient, Gender, Admission

# Path to MIMIC data
DATA_PATH = "/Volumes/T7/OOP_project/Data"

# Load patients
print("Loading patients...")
patients_df = pd.read_csv(f"{DATA_PATH}/PATIENTS.csv", nrows=10)
patients = {}

for _, row in patients_df.iterrows():
    patient = Patient(
        subject_id=int(row['SUBJECT_ID']),
        gender=Gender(row['GENDER']) if row['GENDER'] in ['M', 'F'] else None,
        dob=pd.to_datetime(row['DOB'], errors='coerce'),
        dod=pd.to_datetime(row['DOD'], errors='coerce')
    )
    patients[patient.subject_id] = patient

print(f"✓ Loaded {len(patients)} patients")

# Load admissions
print("Loading admissions...")
admissions_df = pd.read_csv(f"{DATA_PATH}/ADMISSIONS.csv", nrows=10)

for _, row in admissions_df.iterrows():
    admission = Admission(
        hadm_id=int(row['HADM_ID']),
        subject_id=int(row['SUBJECT_ID']),
        admittime=pd.to_datetime(row['ADMITTIME'], errors='coerce'),
        dischtime=pd.to_datetime(row['DISCHTIME'], errors='coerce')
    )
    
    if admission.subject_id in patients:
        admission.patient = patients[admission.subject_id]
        patients[admission.subject_id].admissions.append(admission)

print(f"✓ Loaded admissions")

# Show first patient
if patients:
    patient = list(patients.values())[0]
    print(f"\nFirst patient: {patient.subject_id}")
    print(f"  Gender: {patient.gender}")
    print(f"  Admissions: {len(patient.admissions)}")

print("\n✅ Done!")
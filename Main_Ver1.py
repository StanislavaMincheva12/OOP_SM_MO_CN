import pandas as pd
import matplotlib.pyplot as plt

callout = pd.read_csv("CALLOUT.csv")
admissions = pd.read_csv("ADMISSIONS.csv")
caregivers = pd.read_csv("CAREGIVERS.csv")
chartevents = pd.read_csv("CHARTEVENTS.csv")
cpt_events = pd.read_csv("CPTEVENTS.csv")
d_cpt = pd.read_csv("D_CPT.csv")
d_icd_diagnoses = pd.read_csv("D_ICD_DIAGNOSES.csv") 
d_icd_procedures = pd.read_csv("D_ICD_PROCEDURES.csv")
d_items = pd.read_csv("D_ITEMS.csv")
diagnoses_icd = pd.read_csv("DIAGNOSES_ICD.csv") 
drgcodes = pd.read_csv("DRGCODES.csv") 
icustays = pd.read_csv("ICUSTAYS.csv") 
inputevents_mv = pd.read_csv("INPUTEVENTS_MV.csv") 
inputevents_cv = pd.read_csv("INPUTEVENTS_CV.csv") 
labevents = pd.read_csv("LABEVENTS.csv") 
microbiologyevents = pd.read_csv("MICROBIOLOGYEVENTS.csv") 
noteevents = pd.read_csv("NOTEEVENTS.csv") 
outputevents = pd.read_csv("OUTPUTEVENTS.csv") 
prescriptions = pd.read_csv("PRESCRIPTIONS.csv") 
procedures_icd = pd.read_csv("PROCEDURES_ICD.csv") 
procedures_events_mv= pd.read_csv("PROCEDUREEVENTS_MV.csv") 
services = pd.read_csv("SERVICES.csv") 
transfers = pd.read_csv("TRANSFERS.csv")
patients = pd.read_csv("PATIENTS.csv")

from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
from collections import defaultdict


@dataclass
class Patient:
    subject_id: int          # PATIENTS.SUBJECT_ID
    gender: Optional[str] = None  # PATIENTS.GENDER
    dob: Optional[datetime] = None  # PATIENTS.Day of birth 
    dod: Optional[datetime] = None  # PATIENTS.Day of death 

@dataclass  
class Admission:
    hadm_id: int             # ADMISSIONS.HADM_ID
    subject_id: int          # ADMISSIONS.SUBJECT_ID
    admittime: Optional[datetime] = None  # ADMISSIONS.ADMITTIME
    dischtime: Optional[datetime] = None  # ADMISSIONS.DISCHTIME

@dataclass
class Microbiology:
    row_id: int              # MICROBIOLOGYEVENTS.ROW_ID
    hadm_id: int             # MICROBIOLOGYEVENTS.HADM_ID  
    icustay_id: Optional[int] = None  # MICROBIOLOGYEVENTS.ICUSTAY_ID
    org_itemid: Optional[int] = None  # MICROBIOLOGYEVENTS.ORG_ITEMID
    org_name: Optional[str] = None    # MICROBIOLOGYEVENTS.ORG_NAME
    charttime: Optional[datetime] = None  # MICROBIOLOGYEVENTS.CHARTTIMe

@dataclass(frozen=True)
class Unit:
    unit_id: str  # e.g. 'MICU', 'SICU' == FIRST_CAREUNIT/LAST_CAREUNIT

@dataclass(frozen=True)
class WardLocation:
    ward_id: int  # FIRST_WARDID/LAST_WARDID (physical location id)

@dataclass
class ICUStay:
    icustay_id: int                 # ICUSTAYS.ICUSTAY_ID
    subject_id: int                 # ICUSTAYS.SUBJECT_ID
    hadm_id: int                    # ICUSTAYS.HADM_ID
    dbsource: Optional[str] = None  # ICUSTAYS.DBSOURCE
    first_unit: Optional[Unit] = None
    last_unit: Optional[Unit] = None
    first_ward: Optional[WardLocation] = None
    last_ward: Optional[WardLocation] = None
    intime: Optional[datetime] = None
    outtime: Optional[datetime] = None
    los: Optional[float] = None


class Staff:
    def __init__(self, cgid: int):  # CAREGIVERS.CGID
        self.cgid = cgid
        self.label: Optional[str] = None  # CAREGIVERS.LABEL

class Ward:
    def __init__(self, first_careunit: str, parent_unit):  # ICUSTAYS.FIRST_CAREUNIT
        self.ward_id = first_careunit  # e.g. 'MICU'
        self.parent_unit = parent_unit

class Unit:
    def __init__(self, unit_name: str):
        self.unit_id = unit_name  # e.g. 'MEDICAL_INTENSIVE'

# Capitalize column names
microbiologyevents.columns = microbiologyevents.columns.str.upper()
icustays.columns = icustays.columns.str.upper()

# Merge microbiology with ICU stays to get ward and unit info
micro_merged = microbiologyevents.merge(
    icustays[['HADM_ID', 'FIRST_WARDID', 'FIRST_CAREUNIT']],
    on='HADM_ID',
    how='left'
)

# Count positive tests per ward and specimen
ward_spec = micro_merged.groupby(['FIRST_WARDID', 'SPEC_ITEMID', 'ORG_NAME', 'FIRST_CAREUNIT']).size().reset_index(name='positive_tests')
ward_spec.columns = ['WARD_ID', 'SPECIMEN_ID', 'SPECIMEN_NAME', 'CARE_UNIT', 'POSITIVE_TESTS']
ward_spec['ALERT'] = ward_spec['POSITIVE_TESTS'] > 5
ward_spec = ward_spec.sort_values('POSITIVE_TESTS', ascending=False)

# Reorder columns
ward_spec = ward_spec[['WARD_ID', 'SPECIMEN_ID', 'POSITIVE_TESTS', 'SPECIMEN_NAME', 'ALERT', 'CARE_UNIT']]

# Display results
print("MICROBIOLOGY POSITIVE TESTS BY WARD AND SPECIMEN")
print("-"*90)

print("\nALERTS - Specimens with > 5 positive tests per ward:")
print("-"*90)
alerts = ward_spec[ward_spec['ALERT']]
if len(alerts) > 0:
    print(alerts.to_string(index=False))
else:
    print("No alerts")

print("\n\nComplete Summary (All Ward-Specimen Combinations):")
print("-"*90)
print(ward_spec.to_string(index=False))
# Count affected wards per unit
wards_per_unit = ward_spec.groupby('CARE_UNIT')['WARD_ID'].nunique().reset_index(name='affected_wards')
wards_per_unit['ALERT'] = wards_per_unit['affected_wards'] > 2

print("ALERT: UNITS WITH MORE THAN 2 AFFECTED WARDS")
print("-"*90)

unit_alerts = wards_per_unit[wards_per_unit['ALERT']]
if len(unit_alerts) > 0:
    print("\n UNIT-LEVEL ALERTS (> 2 affected wards):")
    print("-"*90)
    for _, row in unit_alerts.iterrows():
        unit = row['CARE_UNIT']
        num_wards = row['affected_wards']
        affected_wards = ward_spec[ward_spec['CARE_UNIT'] == unit]['WARD_ID'].unique()
        print(f"\n  UNIT: {unit}")
        print(f"  Affected Wards: {num_wards} → {sorted([int(w) for w in affected_wards if pd.notna(w)])}")
        print(f"  Specimens detected:")
        unit_data = ward_spec[ward_spec['CARE_UNIT'] == unit]
        for _, spec_row in unit_data.iterrows():
            print(f"    - Ward {int(spec_row['WARD_ID'])}: {int(spec_row['POSITIVE_TESTS'])} tests of {spec_row['SPECIMEN_NAME']}")
else:
    print("\n✓ No unit-level alerts: All units have ≤ 2 affected wards")

# Count unique wards and units from ICU stays
total_wards = icustays['FIRST_WARDID'].nunique()
total_units = icustays['FIRST_CAREUNIT'].nunique()

# Get list of unit names
units_list = sorted(icustays['FIRST_CAREUNIT'].dropna().unique())

print(f"\nTotal Units: {total_units}")
print(f"Units: {', '.join(units_list)}")

print(f"\nTotal Wards: {total_wards}")
wards_list = sorted(icustays['FIRST_WARDID'].dropna().unique())
print(f"Wards: {', '.join([str(int(w)) for w in wards_list])}")


# Get the unit-ward mapping from ICU stays
unit_ward_map = icustays[['FIRST_CAREUNIT', 'FIRST_WARDID']].drop_duplicates()
unit_ward_map = unit_ward_map.dropna()

# Count wards per unit
wards_per_unit_detail = unit_ward_map.groupby('FIRST_CAREUNIT')['FIRST_WARDID'].count().reset_index()
wards_per_unit_detail.columns = ['UNIT', 'NUM_WARDS']
wards_per_unit_detail = wards_per_unit_detail.sort_values('NUM_WARDS', ascending=False)

print("\nWards per Unit:")
print(wards_per_unit_detail.to_string(index=False))

print("\n\nDetailed Mapping (Unit → Wards):")
print("-"*70)
for unit in sorted(icustays['FIRST_CAREUNIT'].dropna().unique()):
    wards = sorted(icustays[icustays['FIRST_CAREUNIT'] == unit]['FIRST_WARDID'].dropna().unique())
    print(f"\n{unit:10s}: {len(wards):2d} wards → {[int(w) for w in wards]}")

micro_merged['ORG_UPPER'] = micro_merged['ORG_NAME'].str.upper()
high_priority = micro_merged[
    micro_merged['ORG_UPPER'].str.contains('|'.join([o.upper() for o in HIGH_PRIORITY]), case=False, na=False)
]

if len(high_priority) > 0:
    print(f"\n CRITICAL: {len(high_priority)} high-priority organism detections!")
    summary = high_priority.groupby(['ORG_NAME', 'FIRST_WARDID', 'FIRST_CAREUNIT']).size().reset_index(name='count')
    for _, row in summary.iterrows():
        print(f"  {row['ORG_NAME']:40s} | Ward {int(row['FIRST_WARDID']):2d} ({row['FIRST_CAREUNIT']}) | {row['count']} cases")
else:
    print("\n✓ No high-priority pathogens detected")

# 2. RARE ORGANISM DETECTION
print("\n\n2. RARE/NOVEL ORGANISM DETECTION")
print("-"*90)

org_freq = microbiologyevents.groupby('ORG_NAME').size().reset_index(name='count')
org_freq = org_freq.sort_values('count', ascending=True)
rare = org_freq[org_freq['count'] <= 2]

print(f"\nRare organisms (1-2 detections): {len(rare)}")
if len(rare) > 0:
    print("\n NOVEL ORGANISMS:")
    for _, row in rare.iterrows():
        details = micro_merged[micro_merged['ORG_NAME'] == row['ORG_NAME']][['FIRST_WARDID', 'FIRST_CAREUNIT']].drop_duplicates()
        for _, d in details.iterrows():
            print(f"  • {row['ORG_NAME']:40s} | Ward {int(d['FIRST_WARDID']):2d} ({d['FIRST_CAREUNIT']})")

# 3. FREQUENCY ANALYSIS
print("\n\n3. ORGANISM FREQUENCY ANALYSIS")
print("-"*90)
print("\nTop 10 most common organisms:")
for _, row in org_freq.tail(10).sort_values('count', ascending=False).iterrows():
    print(f"  {row['ORG_NAME']:40s}: {row['count']:4d} cases")

print(f"\nOrganisms distribution: {len(org_freq)} unique | Rare(1-2): {len(rare)} | Common(>6): {len(org_freq[org_freq['count'] >= 6])}")
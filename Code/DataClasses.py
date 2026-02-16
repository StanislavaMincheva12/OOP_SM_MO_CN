from dataclasses import dataclass, field
from typing import List, Optional, Dict
from datetime import datetime
from collections import defaultdict
""" This is just meta structure of all classes and basic hierarcy"""

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


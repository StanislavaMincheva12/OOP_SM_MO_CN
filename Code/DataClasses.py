"""
MIMIC-III Unified Data Models - Simplified
Merges the original versions with minimal enhancements.
Aligns with actual MIMIC III schema.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime
from enum import Enum

# genotypical sex, so only 2
class Gender(str, Enum):
    MALE = "M"
    FEMALE = "F"


class CareUnit(str, Enum):
    MICU = "MICU"
    SICU = "SICU"
    CSRU = "CSRU"
    CCU = "CCU"
    TSICU = "TSICU"

class AdmissionType(str, Enum):
    EMERGENCY = "EMERGENCY"
    URGENT = "URGENT"
    ELECTIVE = "ELECTIVE"
    NEWBORN = "NEWBORN"

@dataclass
class Entity(ABC):
    """Base entity with common functionality"""
    unique_id: int | str
    
    @abstractmethod
    def get_id(self) -> int | str:
        return self.unique_id
    
    def __hash__(self):
        return hash(self.get_id())
    
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self.get_id() == other.get_id()

@dataclass
class Organism(Entity, ABC):
    """Parent class for biological entities"""
    name: str = "Unknown"
    
    @abstractmethod
    def get_risk_level(self) -> float:
        pass


@dataclass
class Person(Organism, ABC):
    """Parent class for human entities"""
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    
    def get_risk_level(self) -> float:
        return 0.0

@dataclass
class Patient(Person):
    """Patient from MIMIC PATIENTS table"""
    subject_id: int = 0
    gender: Optional[Gender] = None
    dob: Optional[datetime] = None
    dod: Optional[datetime] = None
    expire_flag: Optional[bool] = None
    
    # Domain attributes
    infections: List['Pathogen'] = field(default_factory=list)
    contagious: bool = False
    ward: Optional['Ward'] = None
    admissions: List['Admission'] = field(default_factory=list)
    icu_stays: List['ICUStay'] = field(default_factory=list)
    
    def __post_init__(self):
        self.unique_id = self.subject_id
    
    def get_id(self) -> int:
        return self.subject_id
    
    @property
    def is_deceased(self) -> bool:
        return self.dod is not None or self.expire_flag is True
    
    def infect(self, pathogen: 'Pathogen') -> None:
        """Infect patient and update ward risk"""
        if pathogen not in self.infections:
            self.infections.append(pathogen)
        
        if pathogen.risk_rate > 5:
            self.contagious = True
        
        if self.ward:
            self.ward.update_risk()

@dataclass
class Staff(Person):
    """Staff/Caregiver from MIMIC CAREGIVERS table"""
    cgid: int = 0
    label: Optional[str] = None
    
    def __post_init__(self):
        self.unique_id = self.cgid
    
    def get_id(self) -> int:
        return self.cgid


@dataclass
class Pathogen(Organism):
    """Microorganism"""
    org_itemid: Optional[int] = None
    org_name: Optional[str] = None
    risk_rate: float = 3.0
    is_resistant: bool = False
    
    def __post_init__(self):
        self.unique_id = self.org_itemid if self.org_itemid else hash(self.org_name)
    
    def get_id(self) -> int | str:
        return self.unique_id
    
    def get_risk_level(self) -> float:
        risk = self.risk_rate
        if self.is_resistant:
            risk *= 1.5
        return min(risk, 10.0)

@dataclass
class Ward(Entity):
    """Ward - part of ICU"""
    ward_id: int
    icu: Optional['ICU'] = None
    patients: List[Patient] = field(default_factory=list)
    risk_level: float = 0.0
    
    def __post_init__(self):
        self.unique_id = self.ward_id
    
    def get_id(self) -> int:
        return self.ward_id
    
    def add_patient(self, patient: Patient) -> None:
        """Add patient to ward"""
        self.patients.append(patient)
        patient.ward = self
        self.update_risk()
    
    def update_risk(self) -> None:
        """Update ward risk based on contagious patients"""
        contagious_count = sum(1 for p in self.patients if p.contagious)
        self.risk_level = contagious_count
        
        if self.icu:
            self.icu.update_risk()

@dataclass
class ICU(Entity):
    """ICU/Care Unit"""
    name: str
    unit_type: CareUnit = CareUnit.MICU
    wards: List[Ward] = field(default_factory=list)
    risk_level: float = 0.0
    hospital: Optional['Hospital'] = None
    
    def __post_init__(self):
        self.unique_id = self.name
    
    def get_id(self) -> str:
        return self.name
    
    def add_ward(self, ward: Ward) -> None:
        """Add ward to ICU"""
        self.wards.append(ward)
        ward.icu = self
        self.update_risk()
    
    def update_risk(self) -> None:
        """Update ICU risk based on ward risks"""
        if self.wards:
            self.risk_level = sum(w.risk_level for w in self.wards)
        
        if self.hospital:
            self.hospital.update_risk()

@dataclass
class Hospital(Entity):
    """Hospital containing ICUs"""
    name: str
    icus: List[ICU] = field(default_factory=list)
    patients: dict = field(default_factory=dict)
    risk_level: float = 0.0
    
    def __post_init__(self):
        if self.unique_id is None:
            self.unique_id = self.name
    
    def get_id(self) -> str:
        return self.name
    
    def add_icu(self, icu: ICU) -> None:
        """Add ICU to hospital"""
        self.icus.append(icu)
        icu.hospital = self
        self.update_risk()
    
    def add_patient(self, patient: Patient) -> None:
        """Add patient to hospital registry"""
        self.patients[patient.subject_id] = patient
    
    def update_risk(self) -> None:
        """Update hospital risk based on ICU risks"""
        if self.icus:
            self.risk_level = sum(i.risk_level for i in self.icus) / len(self.icus)


@dataclass
class Admission:
    """MIMIC ADMISSIONS table"""
    hadm_id: int
    subject_id: int
    admittime: Optional[datetime] = None
    dischtime: Optional[datetime] = None
    admission_type: Optional[AdmissionType] = None
    deathtime: Optional[datetime] = None
    hospital_expire_flag: Optional[bool] = None
    
    patient: Optional[Patient] = None
    icu_stays: List['ICUStay'] = field(default_factory=list)

@dataclass
class ICUStay:
    """MIMIC ICUSTAYS table"""
    icustay_id: int
    subject_id: int
    hadm_id: int
    first_careunit: Optional[CareUnit] = None
    last_careunit: Optional[CareUnit] = None
    first_wardid: Optional[int] = None
    last_wardid: Optional[int] = None
    intime: Optional[datetime] = None
    outtime: Optional[datetime] = None
    los: Optional[float] = None
    
    patient: Optional[Patient] = None
    admission: Optional[Admission] = None

@dataclass
class Microbiology:
    """MIMIC MICROBIOLOGYEVENTS table"""
    row_id: int
    hadm_id: int
    icustay_id: Optional[int] = None
    subject_id: Optional[int] = None
    spec_type: Optional[str] = None
    org_name: Optional[str] = None
    org_itemid: Optional[int] = None
    charttime: Optional[datetime] = None
    interpretation: Optional[str] = None
    
    admission: Optional[Admission] = None
    patient: Optional[Patient] = None


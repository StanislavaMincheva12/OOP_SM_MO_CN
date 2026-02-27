from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict


class Alert(ABC):

    @property
    @abstractmethod
    def id(self) -> int: ...

    @abstractmethod
    def should_raise(self) -> bool: ...

    @abstractmethod
    def describe(self) -> str: ...


@dataclass
class Pathogen:
    org_id: Optional[int]
    org_name: str
    danger_weight: float
    time_window_days: int
    ward_thresholds: Dict[str, int] 
    staff_threshold: int

    def get_ward_threshold(self, ward_size: int) -> int:
        if ward_size <= 5:
            return self.ward_thresholds["5"]
        elif ward_size <= 10:
            return self.ward_thresholds["10"]
        else:
            return self.ward_thresholds["20"]


@dataclass
class MicrobiologyAlert(Alert):
    counter_id: int
    pathogen: Pathogen
    ward_id: int
    ward_size: int
    start_time: datetime
    alert_type: str
    curr_patient_number: int

    @property
    def id(self) -> int:
        return self.counter_id

    @property
    def org_id(self) -> Optional[int]:
        return self.pathogen.org_id

    def should_raise(self) -> bool:
        return self.curr_patient_number >= self.pathogen.get_ward_threshold(self.ward_size)

    def describe(self) -> str:
        thresh = self.pathogen.get_ward_threshold(self.ward_size)
        status = "⚠ ALERT" if self.should_raise() else "OK"
        return (
            f"[{status}][{self.alert_type}] Ward {self.ward_id} "
            f"({self.ward_size} beds) | {self.pathogen.org_name} | "
            f"{self.curr_patient_number}/{thresh} patients @ {self.start_time}"
        )


@dataclass
class WardAlert(Alert):
    counter_id: int
    ward_id: int
    pathogen: Pathogen
    ward_size: int
    start_time: datetime
    alert_type: str
    curr_patient_number: int

    @property
    def id(self) -> int:
        return self.counter_id

    def should_raise(self) -> bool:
        return self.curr_patient_number >= self.pathogen.get_ward_threshold(self.ward_size)

    def describe(self) -> str:
        thresh = self.pathogen.get_ward_threshold(self.ward_size)
        status = "⚠ ALERT" if self.should_raise() else "OK"
        return (
            f"[{status}][{self.alert_type}] Ward {self.ward_id} "
            f"({self.ward_size} beds) | {self.pathogen.org_name} | "
            f"{self.curr_patient_number}/{thresh} patients @ {self.start_time}"
        )


PATHOGEN_REGISTRY = {
    "CLOSTRIDIUM DIFFICILE": Pathogen(
        org_id=None, org_name="CLOSTRIDIUM DIFFICILE",
        danger_weight=3.0, time_window_days=3,
        ward_thresholds={"5": 1, "10": 1, "20": 1},
        staff_threshold=2,
    ),
    "GRAM NEGATIVE ROD": Pathogen(
        org_id=None, org_name="GRAM NEGATIVE ROD",
        danger_weight=1.5, time_window_days=2,
        ward_thresholds={"5": 1, "10": 1, "20": 2},
        staff_threshold=3,
    ),
    "CANDIDA ALBICANS": Pathogen(
        org_id=None, org_name="CANDIDA ALBICANS",
        danger_weight=1.0, time_window_days=2,
        ward_thresholds={"5": 1, "10": 1, "20": 2},
        staff_threshold=3,
    ),
    "STREPTOCOCCUS": Pathogen(
        org_id=None, org_name="STREPTOCOCCUS",
        danger_weight=1.0, time_window_days=2,
        ward_thresholds={"5": 1, "10": 2, "20": 3},
        staff_threshold=4,
    ),
    "HAEMOPHILUS INFLUENZAE": Pathogen(
        org_id=None, org_name="HAEMOPHILUS INFLUENZAE",
        danger_weight=1.0, time_window_days=2,
        ward_thresholds={"5": 1, "10": 1, "20": 2},
        staff_threshold=3,
    ),
    "YEAST unspeciated": Pathogen(
        org_id=None, org_name="YEAST unspeciated",
        danger_weight=1.0, time_window_days=3,
        ward_thresholds={"5": 1, "10": 1, "20": 3},
        staff_threshold=4,
    ),
}


import pandas as pd
import matplotlib.pyplot as plt

admissions = pd.read_csv("/Volumes/T7/OOP_project/Data/ADMISSIONS.csv")
caregivers = pd.read_csv("/Volumes/T7/OOP_project/Data/CAREGIVERS.csv")
chartevents = pd.read_csv("/Volumes/T7/OOP_project/Data/CHARTEVENTS.csv")
d_items = pd.read_csv("/Volumes/T7/OOP_project/Data/D_ITEMS.csv")
icustays = pd.read_csv("/Volumes/T7/OOP_project/Data/ICUSTAYS.csv") 
microbiologyevents = pd.read_csv("/Volumes/T7/OOP_project/Data/MICROBIOLOGYEVENTS.csv") 
noteevents = pd.read_csv("/Volumes/T7/OOP_project/Data/NOTEEVENTS.csv") 
outputevents = pd.read_csv("/Volumes/T7/OOP_project/Data/OUTPUTEVENTS.csv") 
transfers = pd.read_csv("/Volumes/T7/OOP_project/Data/TRANSFERS.csv")
patients = pd.read_csv("/Volumes/T7/OOP_project/Data/PATIENTS.csv")
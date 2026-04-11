from abc import ABC, abstractmethod
from dataclasses import dataclass
from microbiology.pathogens import Pathogen
from datetime import datetime
from typing import Optional

"""
This module defines Alert class and its child classes, 
would be very useful if in the future the project is updated to have 
further alerts, as in the beginning we planned to have staff alerts. 
 """

class Alert(ABC):

    """
    Abstract class for alerts.
    For now only one child class is implemented -> Microbiology alert.
    """

    @property
    @abstractmethod
    def id(self) -> int: ...

    @abstractmethod
    def should_raise(self) -> bool: ...

    @abstractmethod
    def describe(self) -> str: ...


@dataclass
class MicrobiologyAlert(Alert):

    """
    Class for alerts related to microbiology episodes occurying in hospitals.
    """

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
        status = "ALERT" if self.should_raise() else "OK"
        return (
            f"[{status}][{self.alert_type}] Ward {self.ward_id} "
            f"({self.ward_size} beds) | {self.pathogen.org_name} | "
            f"{self.curr_patient_number}/{thresh} patients @ {self.start_time}"
        )

    def to_dict(self):
        return {
            "ALERT_ID": self.id,
            "WARD_ID": self.ward_id,
            "ORG_ID": self.org_id,
            "ORG_NAME": self.pathogen.key,
            "NUM_PATIENTS": self.curr_patient_number,
            "START_TIME": self.start_time,
            "SEVERITY": int(self.pathogen.danger_weight * 10),
            "THRESHOLD": self.pathogen.get_ward_threshold(self.ward_size),
            "WINDOW_DAYS": self.pathogen.time_window_days
        }
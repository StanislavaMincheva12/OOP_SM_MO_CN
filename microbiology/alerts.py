"""This file defines the Alert classes used in the microbiology alert system.

"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from microbiology.pathogens import Pathogen
from datetime import datetime
from typing import Optional

class Alert(ABC):

    @property
    @abstractmethod
    def id(self) -> int: ...

    @abstractmethod
    def should_raise(self) -> bool: ...

    @abstractmethod
    def describe(self) -> str: ...


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
        status = "ALERT" if self.should_raise() else "OK"
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
        status = "ALERT" if self.should_raise() else "OK"
        return (
            f"[{status}][{self.alert_type}] Ward {self.ward_id} "
            f"({self.ward_size} beds) | {self.pathogen.org_name} | "
            f"{self.curr_patient_number}/{thresh} patients @ {self.start_time}"
        )

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional
import pandas as pd
from microbiology.patients import Patient
from microbiology.pathogens import Pathogen, PathogenRegistry


class Ward:
    def __init__(self, ward_id: int, ward_size: int):
        self.ward_id = ward_id
        self.ward_size = ward_size
        self.patients: List[Patient] = []

    def add_patient(self, patient: Patient) -> None:
        self.patients.append(patient)

    def get_patients_by_pathogen(self, pathogen: Pathogen) -> List[Patient]:
        return [
            patient for patient in self.patients
            if patient.pathogen.key == pathogen.key
        ]

    def has_outbreak(self, pathogen: Pathogen) -> bool:
        infected = len(self.get_patients_by_pathogen(pathogen))
        threshold = pathogen.get_ward_threshold(self.ward_size)
        return infected >= threshold

    def total_capacity(self) -> int:
        return self.ward_size

    def total_patients(self) -> int:
        return len(self.patients)

    def __repr__(self) -> str:
        return f"Ward({self.ward_id}, size={self.ward_size}, patients={len(self.patients)})"


class Department:
    def __init__(self, name: str):
        self.name = name
        self.wards: Dict[int, Ward] = {}

    def add_ward(self, ward: Ward) -> None:
        self.wards[ward.ward_id] = ward

    def get_ward(self, ward_id: int) -> Optional[Ward]:
        return self.wards.get(ward_id)

    def total_capacity(self) -> int:
        return sum(ward.total_capacity() for ward in self.wards.values())

    def total_patients(self) -> int:
        return sum(ward.total_patients() for ward in self.wards.values())

    def has_outbreak(self, pathogen: Pathogen) -> bool:
        return any(ward.has_outbreak(pathogen) for ward in self.wards.values())

    def all_wards(self) -> List[Ward]:
        return list(self.wards.values())

    def __repr__(self) -> str:
        return f"Department({self.name}, wards={len(self.wards)})"


class Hospital:
    def __init__(self, name: str = "Hospital"):
        self.name = name
        self.departments: Dict[str, Department] = {}

    def add_department(self, department: Department) -> None:
        self.departments[department.name] = department

    def add_ward(self, ward: Ward, department_name: str = "General") -> None:
        department = self.departments.get(department_name)
        if department is None:
            department = Department(department_name)
            self.add_department(department)
        department.add_ward(ward)

    def get_department(self, name: str) -> Optional[Department]:
        return self.departments.get(name)

    def get_ward(self, ward_id: int) -> Optional[Ward]:
        for department in self.departments.values():
            ward = department.get_ward(ward_id)
            if ward is not None:
                return ward
        return None

    def all_wards(self) -> List[Ward]:
        return [ward for department in self.departments.values() for ward in department.all_wards()]

    def total_capacity(self) -> int:
        return sum(department.total_capacity() for department in self.departments.values())

    def total_patients(self) -> int:
        return sum(department.total_patients() for department in self.departments.values())

    def has_outbreak(self, pathogen: Pathogen) -> bool:
        return any(department.has_outbreak(pathogen) for department in self.departments.values())

    def outbreak_summary(self, pathogens: list[Pathogen]) -> Dict[str, Dict[int, list[str]]]:
        summary: Dict[str, Dict[int, list[str]]] = {}
        for department in self.departments.values():
            ward_alerts: Dict[int, list[str]] = {}
            for ward in department.all_wards():
                pathogens_in_alert = [pathogen.key for pathogen in pathogens if ward.has_outbreak(pathogen)]
                if pathogens_in_alert:
                    ward_alerts[ward.ward_id] = pathogens_in_alert
            if ward_alerts:
                summary[department.name] = ward_alerts
        return summary

    @classmethod
    def from_dataframe(cls, records: pd.DataFrame, registry: PathogenRegistry, department_name: str = "ICU") -> "Hospital":
        hospital = cls(name="MIMIC Hospital")
        for _, row in records.iterrows():
            pathogen = registry.get(row["ORG_NAME"])
            if pathogen is None:
                continue

            patient = Patient(
                patient_id=int(row["SUBJECT_ID"]),
                ward_id=int(row["WARD_ID"]),
                pathogen_name=pathogen,
            )

            ward = hospital.get_ward(int(row["WARD_ID"]))
            if ward is None:
                ward = Ward(int(row["WARD_ID"]), int(row["WARD_SIZE"]))
                hospital.add_ward(ward, department_name)

            ward.add_patient(patient)

        return hospital

    def __repr__(self) -> str:
        return f"Hospital({self.name}, departments={len(self.departments)})"

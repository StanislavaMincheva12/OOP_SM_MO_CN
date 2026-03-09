from dataclasses import dataclass
import pandas as pd


@dataclass
class DataRepository:

    """
    Stores all tables in one object. So tables can be called using object.table_name in main.py.
    """
    patients: pd.DataFrame
    caregivers: pd.DataFrame
    d_items: pd.DataFrame
    admissions: pd.DataFrame
    icustays: pd.DataFrame
    noteevents: pd.DataFrame
    microbiologyevents: pd.DataFrame
    transfers: pd.DataFrame
    outputevents: pd.DataFrame
    chartevents: pd.DataFrame

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            patients=data["PATIENTS"],
            caregivers=data["CAREGIVERS"],
            d_items=data["D_ITEMS"],
            admissions=data["ADMISSIONS"],
            icustays=data["ICUSTAYS"],
            noteevents=data["NOTEEVENTS"],
            microbiologyevents=data["MICROBIOLOGYEVENTS"],
            transfers=data["TRANSFERS"],
            outputevents=data["OUTPUTEVENTS"],
            chartevents=data["CHARTEVENTS"],
        )
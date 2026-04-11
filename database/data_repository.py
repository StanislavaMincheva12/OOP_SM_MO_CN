from __future__ import annotations

from typing import Optional
import pandas as pd
from database.db_connector_loader import TableLoader

"""This module is defining and storing the microbiology data repository"""


class DataRepository:

    """
    Repository for accessing tabular data with lazy loading and caching.

    Data can be provided either as pandas DataFrames or loaded on demand
    from a database via a TableLoader.
    
    """

    TABLES = [
        "PATIENTS", "CAREGIVERS", "D_ITEMS", "ADMISSIONS", "ICUSTAYS",
        "NOTEEVENTS", "MICROBIOLOGYEVENTS", "TRANSFERS", "OUTPUTEVENTS", "CHARTEVENTS"
    ]

    def __init__(self, loader: Optional[TableLoader] = None, data: Optional[dict[str, pd.DataFrame]] = None):
        self._loader = loader
        self._data = data or {}
        self._cache: dict[str, pd.DataFrame] = {}

    @classmethod
    def from_dict(cls, data: dict[str, pd.DataFrame]) -> "DataRepository":
        return cls(data=data)

    @classmethod
    def from_db(cls, db_path: str, tables: list[str]) -> "DataRepository":
        return cls(loader=TableLoader(db_path), data={})

    def _load_table(self, table_name: str) -> pd.DataFrame:
        table_name = table_name.upper()
        if table_name not in self._cache:
            if table_name in self._data:
                self._cache[table_name] = self._data[table_name]
            elif self._loader is not None:
                self._cache[table_name] = self._loader.load_table(table_name)
            else:
                raise KeyError(f"Table '{table_name}' is not available in the repository.")
        return self._cache[table_name]

    def get_table(self, table_name: str) -> pd.DataFrame:
        return self._load_table(table_name)

    def load_all(self) -> None:
        for table in self.TABLES:
            self._load_table(table)

    def __getattr__(self, name: str) -> pd.DataFrame:
        name_upper = name.upper()
        if name_upper in self.TABLES:
            return self._load_table(name_upper)
        raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")

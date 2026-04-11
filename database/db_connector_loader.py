import sqlite3
import pandas as pd
from contextlib import contextmanager

"""
This module connects to the database and loads the data into a dataframe.
"""

class DatabaseConnection:
    """Manages SQLite connection lifecycle as a context manager."""

    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def connect(self):
        """Yield an open connection, always close on exit."""
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def __repr__(self):
        return f"DatabaseConnection(db='{self.db_path}')"



class TableLoader:
    """Loads tables from a SQLite database into DataFrames."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._connection = DatabaseConnection(db_path)

    def list_tables(self) -> list[str]:
        """Return all table names in the database (uppercased)."""
        with self._connection.connect() as conn:
            tables = pd.read_sql_query(
                "SELECT name FROM sqlite_master WHERE type='table'",
                conn
            )
        return tables["name"].str.upper().tolist()

    def load_table(self, table_name: str) -> pd.DataFrame:
        """Load a single table into a DataFrame."""
        with self._connection.connect() as conn:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        return df

    def load_tables(self, table_names: list[str]) -> dict[str, pd.DataFrame]:
        """Load multiple tables into a dict of DataFrames."""
        return {tbl: self.load_table(tbl) for tbl in table_names}

    def __repr__(self):
        return f"TableLoader(db='{self.db_path}')"


# ─── Backwards-compatible functions (so main.py import still works) ───────────

@contextmanager
def get_db_connection(db_path: str):
    """Legacy function — wraps DatabaseConnection.connect()."""
    with DatabaseConnection(db_path).connect() as conn:
        yield conn

def load_tables(db_path: str, table_names: list[str]) -> dict[str, pd.DataFrame]:
    """Legacy function — wraps TableLoader.load_tables()."""
    return TableLoader(db_path).load_tables(table_names)

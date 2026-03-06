import pandas as pd
from .db_connector import get_db_connection


def list_tables(db_path: str):
    '''
    Return tables' names.
    '''
    with get_db_connection(db_path) as conn:
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'",
            conn
        )
    return tables["name"].str.upper().tolist()


def load_table(db_path: str, table_name: str) -> pd.DataFrame:
    '''
    Load a single table function.
    '''
    with get_db_connection(db_path) as conn:
        df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    return df

def load_tables(db_path: str, table_names: list[str]) -> dict[str, pd.DataFrame]:

    '''
    Use prev method to extract all of the tables in one dict. 
    '''

    data = {}

    for tbl in table_names:
        data[tbl] = load_table(db_path, tbl)

    return data
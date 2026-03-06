import sqlite3
from contextlib import contextmanager


@contextmanager
def get_db_connection(db_path: str):

    """
    Just a simple connection to our database. For now is used when loading tables, for example. 
    """

    conn = sqlite3.connect(db_path)
    try:
        yield conn
    finally:
        conn.close()
"""
Dynamic MICROALERTS updates from alerts_df.
Connects log.txt pipeline → DB table.
"""

import pandas as pd
from typing import List, Dict, Any, Optional
from .db_connector_loader import get_db_connection


class MicroalertsRepository:
    """
    Repository pattern for MICROALERTS table operations.
    
    Handles:
    - Table creation and schema management
    - Data insertion and updates
    - Query operations for alert data
    """
    
    TABLE_SCHEMA = """
        CREATE TABLE MICROALERTS (
            ALERT_ID INTEGER,
            WARD_ID INTEGER,
            ORG_ID INTEGER,
            ORG_NAME TEXT,
            NUM_PATIENTS INTEGER,
            START_TIME TEXT,
            SEVERITY INTEGER,
            THRESHOLD INTEGER,
            WINDOW_DAYS INTEGER,
            PRIMARY KEY (WARD_ID, ORG_ID, START_TIME)
        )
    """
    
    def __init__(self, db_path: str):
        """Initialize with database path."""
        self.db_path = db_path
    
    def _execute_query(self, query: str, params: tuple = ()) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as list of dicts.
        
        Args:
            query: SQL query string
            params: Query parameters
            
        Returns:
            List of dictionaries representing rows
        """
        with get_db_connection(self.db_path) as conn:
            df = pd.read_sql_query(query, conn, params=params)
            return df.to_dict('records') if not df.empty else []
    
    def _execute_command(self, command: str, params: tuple = ()) -> None:
        """
        Execute a non-SELECT SQL command.
        
        Args:
            command: SQL command string
            params: Command parameters
        """
        with get_db_connection(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(command, params)
            conn.commit()
    
    def get_alerts_by_ward(self, ward_id: int) -> List[Dict[str, Any]]:
        """
        Get all alerts for a specific ward.
        
        Args:
            ward_id: Ward identifier
            
        Returns:
            List of alert dictionaries
        """
        query = "SELECT * FROM MICROALERTS WHERE WARD_ID = ? ORDER BY START_TIME DESC"
        return self._execute_query(query, (ward_id,))
    
    def get_alerts_by_organism(self, org_id: int) -> List[Dict[str, Any]]:
        """
        Get all alerts for a specific organism.
        
        Args:
            org_id: Organism identifier
            
        Returns:
            List of alert dictionaries
        """
        query = "SELECT * FROM MICROALERTS WHERE ORG_ID = ? ORDER BY START_TIME DESC"
        return self._execute_query(query, (org_id,))
    
    def get_recent_alerts(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get most recent alerts across all wards.
        
        Args:
            limit: Maximum number of alerts to return
            
        Returns:
            List of recent alert dictionaries
        """
        query = "SELECT * FROM MICROALERTS ORDER BY START_TIME DESC LIMIT ?"
        return self._execute_query(query, (limit,))
    
    def get_alert_count(self) -> int:
        """
        Get total number of alerts in the table.
        
        Returns:
            Total alert count
        """
        query = "SELECT COUNT(*) as count FROM MICROALERTS"
        result = self._execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_wards_with_alerts(self) -> List[int]:
        """
        Get list of ward IDs that have active alerts.
        
        Returns:
            List of ward IDs
        """
        query = "SELECT DISTINCT WARD_ID FROM MICROALERTS ORDER BY WARD_ID"
        result = self._execute_query(query)
        return [row['WARD_ID'] for row in result]


class MicroalertsManager:
    """
    Manager class for MICROALERTS table lifecycle operations.
    
    Handles:
    - Table creation and recreation
    - Bulk data updates
    - Table maintenance operations
    """
    
    def __init__(self, db_path: str):
        """Initialize with database path and create repository."""
        self.db_path = db_path
        self.repository = MicroalertsRepository(db_path)
    
    def create_table(self) -> None:
        """
        Create or recreate the MICROALERTS table.
        Drops existing table if it exists.
        """
        with get_db_connection(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS MICROALERTS")
            cur.execute(self.repository.TABLE_SCHEMA)
            conn.commit()
        print("✅ MICROALERTS table recreated")
    
    def update_alerts(self, alerts_df: pd.DataFrame) -> int:
        """
        Update MICROALERTS table with new alerts data.
        
        Args:
            alerts_df: DataFrame containing alert data
            
        Returns:
            Number of alerts updated
        """
        if alerts_df.empty:
            print("No alerts to update.")
            return 0
        
        with get_db_connection(self.db_path) as conn:
            alerts_df.to_sql("MICROALERTS", conn, if_exists="append", index=False)
        
        print(f"✅ Updated {len(alerts_df)} alerts in MICROALERTS")
        return len(alerts_df)
    
    def clear_all_alerts(self) -> int:
        """
        Clear all alerts from the table.
        
        Returns:
            Number of alerts deleted
        """
        count_before = self.repository.get_alert_count()
        self._execute_command("DELETE FROM MICROALERTS")
        count_after = self.repository.get_alert_count()
        deleted = count_before - count_after
        print(f"✅ Cleared {deleted} alerts from MICROALERTS")
        return deleted
    
    def _execute_command(self, command: str, params: tuple = ()) -> None:
        """
        Execute a non-SELECT SQL command.
        
        Args:
            command: SQL command string
            params: Command parameters
        """
        with get_db_connection(self.db_path) as conn:
            cur = conn.cursor()
            cur.execute(command, params)
            conn.commit()


# Backward compatibility wrappers
def create_microalerts_table(db_path: str):
    """Backward compatibility wrapper for procedural code."""
    manager = MicroalertsManager(db_path)
    manager.create_table()


def update_microalerts(alerts_df: pd.DataFrame, db_path: str) -> int:
    """Backward compatibility wrapper for procedural code."""
    manager = MicroalertsManager(db_path)
    return manager.update_alerts(alerts_df)


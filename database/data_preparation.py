import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import pandas as pd
from datetime import timedelta
from typing import Dict, List


"""This module is responsible for prparing the data once it has been loaded from the database"""

class MicrobiologyDataPreparator:
    """
    Object-oriented preparation of microbiology data for alert generation.
    
    Handles:
    - Loading and normalizing data from repository
    - Processing microbiology events by ward
    - Enriching with patient discharge times
    - Generating synthetic sequential dates to restore temporal ordering
    """
    
    # Ward configuration: ID -> capacity mapping
    WARD_CONFIGS = {
        52: 10,
        12: 20,
        50: 10,
        57: 5,
        7: 20,
        23: 15,
        15: 10,
        33: 20,
        14: 10,
    }
    
    SYNTHETIC_START_DATE = pd.to_datetime("2026-03-11")
    
    def __init__(self, data_repo):
        """Initialize with a data repository."""
        self.data_repo = data_repo
        self._microbiologyevents = None
        self._icustays = None
        self._ward_hadm_map = None
    
    @property
    def all_ward_ids(self) -> List[int]:
        """Get list of all monitored ward IDs."""
        return list(self.WARD_CONFIGS.keys())
    
    def _load_and_normalize_data(self) -> None:
        """Load and normalize raw data from repository."""
        if self._microbiologyevents is None:
            self._microbiologyevents = self.data_repo.microbiologyevents.copy()
            self._microbiologyevents.columns = self._microbiologyevents.columns.str.upper()
            self._microbiologyevents["CHARTDATE"] = pd.to_datetime(
                self._microbiologyevents["CHARTDATE"], 
                errors="coerce"
            )
        
        if self._icustays is None:
            self._icustays = self.data_repo.icustays.copy()
            self._icustays.columns = self._icustays.columns.str.upper()
    
    def _build_ward_admission_map(self) -> None:
        """Pre-compute HADM_IDs (admissions) per ward."""
        self._load_and_normalize_data()
        self._ward_hadm_map = {}
        
        for ward_id in self.all_ward_ids:
            hadm_ids = self._icustays.loc[
                (self._icustays["FIRST_WARDID"] == ward_id) | 
                (self._icustays["LAST_WARDID"] == ward_id),
                "HADM_ID"
            ].dropna().unique()
            self._ward_hadm_map[ward_id] = hadm_ids
    
    def _get_ward_microbiology_events(self, ward_id: int) -> pd.DataFrame:
        """
        Extract and clean microbiology events for a specific ward.
        
        Args:
            ward_id: The ward identifier
            
        Returns:
            DataFrame of positive cultures for the ward, sorted by organism and date
        """
        if self._ward_hadm_map is None:
            self._build_ward_admission_map()
        
        ward_config = self.WARD_CONFIGS[ward_id]
        hadm_ids = self._ward_hadm_map[ward_id]
        
        ward_events = self._microbiologyevents[
            self._microbiologyevents["HADM_ID"].isin(hadm_ids) &
            self._microbiologyevents["ORG_NAME"].notna() &
            self._microbiologyevents["CHARTDATE"].notna()
        ].copy()
        
        if len(ward_events) > 0:
            ward_events["ORG_NAME"] = (
                ward_events["ORG_NAME"]
                .astype(str)
                .str.upper()
                .str.strip()
            )
            ward_events["WARD_ID"] = ward_id
            ward_events["WARD_SIZE"] = ward_config
            ward_events = ward_events.sort_values(["ORG_NAME", "CHARTDATE"])
        
        return ward_events
    
    def _process_all_wards(self) -> pd.DataFrame:
        """
        Process microbiology events across all wards.
        
        Returns:
            Combined DataFrame of positive cultures from all wards
        """
        self._load_and_normalize_data()
        all_ward_events = []
        
        for ward_id in self.all_ward_ids:
            print(f"Processing ward {ward_id} (size: {self.WARD_CONFIGS[ward_id]})")
            
            ward_events = self._get_ward_microbiology_events(ward_id)
            
            if len(ward_events) == 0:
                print(f"  No microbiology data for ward {ward_id}")
                continue
            
            all_ward_events.append(ward_events)
            print(f"  Found {len(ward_events)} positive cultures")
        
        return (
            pd.concat(all_ward_events, ignore_index=True) 
            if all_ward_events 
            else pd.DataFrame()
        )
    
    def _get_ward_discharge_info(self, ward_id: int) -> pd.DataFrame:
        """
        Extract discharge times and length of stay for a specific ward.
        
        Args:
            ward_id: The ward identifier
            
        Returns:
            DataFrame with SUBJECT_ID, OUTTIME, LOS for the ward
        """
        if self._ward_hadm_map is None:
            self._build_ward_admission_map()
        
        ward_discharges = self._icustays[
            (self._icustays["FIRST_WARDID"] == ward_id) | 
            (self._icustays["LAST_WARDID"] == ward_id)
        ][["SUBJECT_ID", "OUTTIME", "LOS"]].copy()
        
        ward_discharges["OUTTIME"] = pd.to_datetime(ward_discharges["OUTTIME"])
        ward_discharges["LOS"] = ward_discharges["LOS"].astype('float')
        
        return ward_discharges
    
    def _enrich_with_discharge_times(self, ward_pos_all: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich microbiology events with discharge times and LOS.
        
        Args:
            ward_pos_all: DataFrame of microbiology events
            
        Returns:
            Enriched DataFrame with OUTTIME and LOS
        """
        ward_pos_list = []
        
        for ward_id in self.all_ward_ids:
            ward_pos = ward_pos_all[ward_pos_all["WARD_ID"] == ward_id].copy()
            if len(ward_pos) > 0:
                ward_discharges = self._get_ward_discharge_info(ward_id)
                ward_pos = ward_pos.merge(
                    ward_discharges,
                    on="SUBJECT_ID",
                    how="left",
                    suffixes=("", f"_ward{ward_id}")
                )
                ward_pos_list.append(ward_pos)
        
        return (
            pd.concat(ward_pos_list, ignore_index=True) 
            if ward_pos_list 
            else pd.DataFrame()
        )
    
    def _generate_synthetic_dates(self, ward_pos_all: pd.DataFrame) -> pd.DataFrame:
        """
        Generate synthetic sequential dates to restore temporal ordering.
        
        Args:
            ward_pos_all: DataFrame with microbiology events
            
        Returns:
            DataFrame with synthetic CHARTDATE and OUTTIME
        """
        # Ensure datetime consistency
        ward_pos_all["OUTTIME"] = pd.to_datetime(ward_pos_all["OUTTIME"], errors="coerce")
        ward_pos_all["CHARTDATE"] = pd.to_datetime(ward_pos_all["CHARTDATE"], errors="coerce")
        
        # Get unique patients with LOS
        patients = (
            ward_pos_all[["SUBJECT_ID", "LOS"]]
            .drop_duplicates()
            .sort_values("SUBJECT_ID")
            .reset_index(drop=True)
        )
        
        # Generate sequential CHARTDATE
        patients["CHARTDATE"] = [
            self.SYNTHETIC_START_DATE + timedelta(days=i)
            for i in range(len(patients))
        ]
        
        # OUTTIME = CHARTDATE + LOS
        patients["OUTTIME"] = patients["CHARTDATE"] + pd.to_timedelta(patients["LOS"], unit="D")
        
        # Merge back to main table
        ward_pos_all = ward_pos_all.merge(
            patients[["SUBJECT_ID", "CHARTDATE", "OUTTIME"]],
            on="SUBJECT_ID",
            how="left",
            suffixes=("", "_synthetic")
        )
        
        # Replace old values
        ward_pos_all["CHARTDATE"] = ward_pos_all["CHARTDATE_synthetic"]
        ward_pos_all["OUTTIME"] = ward_pos_all["OUTTIME_synthetic"]
        
        # Cleanup
        ward_pos_all = ward_pos_all.drop(columns=["CHARTDATE_synthetic", "OUTTIME_synthetic"])
        
        return ward_pos_all
    
    def prepare(self) -> pd.DataFrame:
        """
        Main method to prepare microbiology data for alert generation.
        
        Returns:
            Processed DataFrame of ward microbiology events with discharge info
        """
        print("Starting microbiology data preparation...")
        
        # Process all wards
        ward_pos_all = self._process_all_wards()
        
        if ward_pos_all.empty:
            print("No microbiology data found.")
            return ward_pos_all
        
        # Enrich with discharge times
        print("Enriching with discharge information...")
        ward_pos_all = self._enrich_with_discharge_times(ward_pos_all)
        
        # Generate synthetic dates
        print("Generating synthetic sequential dates...")
        ward_pos_all = self._generate_synthetic_dates(ward_pos_all)
        
        print(f"Data preparation complete. Total records: {len(ward_pos_all)}")
        return ward_pos_all


# Backward compatibility wrapper
def prepare_microbiology_data(data_repo):
    """
    Backward compatibility wrapper for procedural code.
    """
    preparator = MicrobiologyDataPreparator(data_repo)
    return preparator.prepare()

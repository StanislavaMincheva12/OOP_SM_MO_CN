import pandas as pd

class MicroAnalysis:
    """
    Micro class for microbiology threshold calculations and alerts.
    Stores analysis results and thresholds for reuse.
    """
    
    def __init__(self, threshold: int = 5, ward_threshold: int = 2):
        self.threshold = threshold      # Specimen alert threshold (>5 positives)
        self.ward_threshold = ward_threshold  # Unit alert threshold (>2 wards)
        self.ward_spec: pd.DataFrame = None  # Ward-specimen summary
        self.unit_summary: pd.DataFrame = None  # Unit-ward summary
    
    def analyze_ward_specimens(self, micro_merged: pd.DataFrame) -> pd.DataFrame:
        """
        Count positive tests per ward/specimen; flag > self.threshold.
        Stores result in self.ward_spec.
        """
        ward_spec = (micro_merged
                     .groupby(['FIRST_WARDID', 'SPEC_ITEMID', 'ORG_NAME', 'FIRST_CAREUNIT'])
                     .size()
                     .reset_index(name='positive_tests'))
        
        ward_spec.columns = ['WARD_ID', 'SPECIMEN_ID', 'SPECIMEN_NAME', 'CARE_UNIT', 'POSITIVE_TESTS']
        ward_spec['ALERT'] = ward_spec['POSITIVE_TESTS'] > self.threshold
        ward_spec = ward_spec.sort_values('POSITIVE_TESTS', ascending=False)
        ward_spec = ward_spec[['WARD_ID', 'SPECIMEN_ID', 'POSITIVE_TESTS', 'SPECIMEN_NAME', 'ALERT', 'CARE_UNIT']]
        
        self.ward_spec = ward_spec
        return ward_spec
    
    def analyze_unit_wards(self) -> pd.DataFrame:
        """Count affected wards per unit; store in self.unit_summary."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first")
        
        unit_summary = (self.ward_spec.groupby('CARE_UNIT')['WARD_ID']
                        .nunique()
                        .reset_index(name='affected_wards'))
        unit_summary['ALERT'] = unit_summary['affected_wards'] > self.ward_threshold
        self.unit_summary = unit_summary
        return unit_summary
    
    def print_ward_reports(self) -> None:
        """Print alerts > threshold and full ward-specimen summary."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first")
            
        print("MICROBIOLOGY POSITIVE TESTS BY WARD AND SPECIMEN")
        print("-"*90)
        
        alerts = self.ward_spec[self.ward_spec['ALERT']]
        print(f"\nALERTS - Specimens with > {self.threshold} positive tests per ward:")
        print("-"*90)
        if len(alerts) > 0:
            print(alerts.to_string(index=False))
        else:
            print("No alerts")
        
        print("\n\nComplete Summary (All Ward-Specimen Combinations):")
        print("-"*90)
        print(self.ward_spec.to_string(index=False))
    
    def print_unit_alerts(self) -> None:
        """Print unit-level alerts (> ward_threshold affected wards)."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first")
            
        print("ALERT: UNITS WITH MORE THAN 2 AFFECTED WARDS")
        print("-"*90)
        
        unit_alerts = self.unit_summary[self.unit_summary['ALERT']] if self.unit_summary is not None else None
        if unit_alerts is not None and len(unit_alerts) > 0:
            print("\n UNIT-LEVEL ALERTS (> 2 affected wards):")
            print("-"*90)
            for _, row in unit_alerts.iterrows():
                unit = row['CARE_UNIT']
                num_wards = row['affected_wards']
                affected_wards = self.ward_spec[self.ward_spec['CARE_UNIT'] == unit]['WARD_ID'].unique()
                affected_wards_clean = sorted([int(w) for w in affected_wards if pd.notna(w)])
                
                print(f"\n  UNIT: {unit}")
                print(f"  Affected Wards: {num_wards} → {affected_wards_clean}")
                print(f"  Specimens detected:")
                unit_data = self.ward_spec[self.ward_spec['CARE_UNIT'] == unit]
                for _, spec_row in unit_data.iterrows():
                    print(f"    - Ward {int(spec_row['WARD_ID'])}: "
                          f"{int(spec_row['POSITIVE_TESTS'])} tests of {spec_row['SPECIMEN_NAME']}")
        else:
            print("\n✓ No unit-level alerts: All units have ≤ 2 affected wards")
    
    def get_alerts(self) -> pd.DataFrame:
        """Quick access: only rows where ALERT=True."""
        return self.ward_spec[self.ward_spec['ALERT']] if self.ward_spec is not None else pd.DataFrame()

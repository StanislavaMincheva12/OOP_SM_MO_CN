import pandas as pd

class MicroDangerousAnalyzer:
    """
    Specialized analyzer for high-priority, rare, and frequency analysis of microbiology data.
    """
    
    def __init__(self, micro_merged: pd.DataFrame, microbiologyevents: pd.DataFrame, 
                 high_priority_list: list = None, rarity_threshold: int = 2):
        self.micro_merged = micro_merged
        self.microbiologyevents = microbiologyevents
        self.high_priority_list = high_priority_list or []
        self.rarity_threshold = rarity_threshold
        self.high_priority = None
        self.rare_orgs = None
        self.org_freq = None
    
    def analyze_high_priority(self) -> pd.DataFrame:
        """Detect high-priority organisms from self.high_priority_list."""
        if not self.high_priority_list:
            return pd.DataFrame()
            
        self.micro_merged['ORG_UPPER'] = self.micro_merged['ORG_NAME'].str.upper()
        high_priority_pattern = '|'.join([o.upper() for o in self.high_priority_list])
        
        high_priority = self.micro_merged[
            self.micro_merged['ORG_UPPER'].str.contains(high_priority_pattern, case=False, na=False)
        ]
        self.high_priority = high_priority
        return high_priority
    
    def print_high_priority_alerts(self) -> None:
        """Print critical high-priority organism summary."""
        high_priority = self.analyze_high_priority()
        
        if len(high_priority) > 0:
            print(f"\n CRITICAL: {len(high_priority)} high-priority organism detections!")
            summary = high_priority.groupby(['ORG_NAME', 'FIRST_WARDID', 'FIRST_CAREUNIT']).size().reset_index(name='count')
            for _, row in summary.iterrows():
                print(f"  {row['ORG_NAME']:40s} | Ward {int(row['FIRST_WARDID']):2d} ({row['FIRST_CAREUNIT']}) | {row['count']} cases")
        else:
            print("\n✓ No high-priority pathogens detected")
    
    def analyze_rare_organisms(self) -> pd.DataFrame:
        """Detect rare organisms (≤ rarity_threshold detections)."""
        org_freq = self.microbiologyevents.groupby('ORG_NAME').size().reset_index(name='count')
        org_freq = org_freq.sort_values('count', ascending=True)
        
        self.org_freq = org_freq
        self.rare_orgs = org_freq[org_freq['count'] <= self.rarity_threshold]
        return self.rare_orgs
    
    def print_rare_and_frequency(self) -> None:
        """Print rare organisms + top-10 + distribution stats."""
        self.analyze_rare_organisms()
        
        print("\n\n2. RARE/NOVEL ORGANISM DETECTION")
        print("-"*90)
        
        print(f"\nRare organisms (1-{self.rarity_threshold} detections): {len(self.rare_orgs)}")
        if len(self.rare_orgs) > 0:
            print("\n NOVEL ORGANISMS:")
            for _, row in self.rare_orgs.iterrows():
                details = self.micro_merged[self.micro_merged['ORG_NAME'] == row['ORG_NAME']][
                    ['FIRST_WARDID', 'FIRST_CAREUNIT']
                ].drop_duplicates()
                for _, d in details.iterrows():
                    print(f"  • {row['ORG_NAME']:40s} | Ward {int(d['FIRST_WARDID']):2d} ({d['FIRST_CAREUNIT']})")
        
        print("\n\n3. ORGANISM FREQUENCY ANALYSIS")
        print("-"*90)
        print("\nTop 10 most common organisms:")
        for _, row in self.org_freq.tail(10).sort_values('count', ascending=False).iterrows():
            print(f"  {row['ORG_NAME']:40s}: {row['count']:4d} cases")
        
        common_count = len(self.org_freq[self.org_freq['count'] >= 6])
        print(f"\nOrganisms distribution: {len(self.org_freq)} unique | "
              f"Rare(1-{self.rarity_threshold}): {len(self.rare_orgs)} | Common(>6): {common_count}")
    
    def get_critical_summary(self) -> pd.DataFrame:
        """Quick access: high-priority summary table."""
        return (self.high_priority.groupby(['ORG_NAME', 'FIRST_WARDID', 'FIRST_CAREUNIT'])
                .size().reset_index(name='count') if self.high_priority is not None else pd.DataFrame())

"""
This is repo with pathogen classes and registry with tresholds.
"""

from dataclasses import dataclass
from typing import Dict, Optional
import pandas as pd

@dataclass(frozen=True)
class Pathogen:
    key: str                     # canonical name / registry key
    org_id: Optional[int]
    danger_weight: float
    time_window_days: int
    ward_thresholds: Dict[int, int]  # ward size breakpoint -> threshold
    staff_threshold: int

    @property
    def org_name(self) -> str:
        """Alias for key, for backward compatibility."""
        return self.key

    def get_ward_threshold(self, ward_size: int) -> int:
        """Largest breakpoint <= ward_size, fallback to max breakpoint."""
        cutoffs = sorted(self.ward_thresholds.keys())
        chosen = max((c for c in cutoffs if c <= ward_size), default=cutoffs[-1])
        return self.ward_thresholds[chosen]
    

class PathogenRegistry:
    def __init__(self):
        self._by_key: Dict[str, Pathogen] = {}
        self._by_org_id: Dict[int, Pathogen] = {}

    def register(self, key: str, **kwargs) -> Pathogen:
        key_norm = key.upper()
        pathogen = Pathogen(key=key_norm, **kwargs)
        self._by_key[key_norm] = pathogen
        if pathogen.org_id is not None:
            self._by_org_id[pathogen.org_id] = pathogen
        return pathogen

    def get(self, key: str) -> Optional[Pathogen]:
        return self._by_key.get(key.upper())

    def get_by_org_id(self, org_id: int) -> Optional[Pathogen]:
        return self._by_org_id.get(org_id)

    def __contains__(self, key: str) -> bool:
        return key.upper() in self._by_key

    def __iter__(self):
        return iter(self._by_key.values())

    def as_dataframe(self) -> pd.DataFrame:
        rows = [
            {
                "key": p.key,
                "org_id": p.org_id,
                "danger_weight": p.danger_weight,
                "time_window_days": p.time_window_days,
                "ward_thresholds": p.ward_thresholds,
                "staff_threshold": p.staff_threshold,
            }
            for p in self._by_key.values()
        ]
        return pd.DataFrame(rows).set_index("key").sort_index()
    
def load_default_pathogens():
    
    REGISTRY = PathogenRegistry()
    # HIGH-RISK (1 case triggers alert - MDR, C. diff, etc.)
    REGISTRY.register("CLOSTRIDIUM DIFFICILE", org_id=None, danger_weight=3.0, time_window_days=3, ward_thresholds={5: 1, 10: 1, 20: 1}, staff_threshold=2)
    REGISTRY.register("ACINETOBACTER BAUMANNII COMPLEX", org_id=None, danger_weight=3.0, time_window_days=3, ward_thresholds={5: 1, 10: 1, 20: 1}, staff_threshold=2)
    REGISTRY.register("ACINETOBACTER BAUMANNII", org_id=None, danger_weight=3.0, time_window_days=3, ward_thresholds={5: 1, 10: 1, 20: 1}, staff_threshold=2)
    REGISTRY.register("POSITIVE FOR METHICILLIN RESISTANT STAPH AUREUS", org_id=None, danger_weight=3.0, time_window_days=1, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=1)
    REGISTRY.register("PSEUDOMONAS AERUGINOSA", org_id=None, danger_weight=3.0, time_window_days=1, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=1)
    REGISTRY.register("KLEBSIELLA PNEUMONIAE", org_id=None, danger_weight=3.0, time_window_days=1, ward_thresholds={5: 1, 10: 1, 20: 1}, staff_threshold=1)
    REGISTRY.register("ESCHERICHIA COLI", org_id=None, danger_weight=3.0, time_window_days=3, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=1)
    REGISTRY.register("STAPH AUREUS COAG +", org_id=None, danger_weight=3.0, time_window_days=1, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=1)
    REGISTRY.register("PROVIDENCIA STUARTII", org_id=None, danger_weight=2.5, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=2)
    REGISTRY.register("STENOTROPHOMONAS (XANTHOMONAS) MALTOPHILIA", org_id=None, danger_weight=2.5, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=2)

    # MEDIUM-RISK (2 cases - Enterobacteriaceae, fungi, anaerobes)
    REGISTRY.register("GRAM NEGATIVE ROD(S)", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=3)
    REGISTRY.register("GRAM NEGATIVE ROD #1", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=3)
    REGISTRY.register("GRAM NEGATIVE ROD #2", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("GRAM NEGATIVE ROD #3", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("GRAM NEGATIVE ROD #4", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("ENTEROBACTERIACEAE", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("KLEBSIELLA OXYTOCA", org_id=None, danger_weight=2.5, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("PROTEUS MIRABILIS", org_id=None, danger_weight=1.5, time_window_days=1, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=3)
    REGISTRY.register("PROTEUS SPECIES", org_id=None, danger_weight=1.5, time_window_days=1, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=3)
    REGISTRY.register("SERRATIA MARCESCENS", org_id=None, danger_weight=2.5, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("MORGANELLA MORGANII", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("PROVIDENCIA RETTGERI", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ASPERGILLUS SP. NOT FUMIGATUS, FLAVUS OR NIGER", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=2)
    REGISTRY.register("MYCELIA STERILIA", org_id=None, danger_weight=1.0, time_window_days=4, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=4)
    REGISTRY.register("BACTEROIDES FRAGILIS GROUP", org_id=None, danger_weight=1.5, time_window_days=3, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ANAEROBIC GRAM POSITIVE ROD(S)", org_id=None, danger_weight=1.5, time_window_days=3, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)

    # LOW-RISK (3+ cases - colonizers, skin flora)
    REGISTRY.register("STAPHYLOCOCCUS, COAGULASE NEGATIVE", org_id=None, danger_weight=1.5, time_window_days=1, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("YEAST", org_id=None, danger_weight=1.0, time_window_days=3, ward_thresholds={5: 1, 10: 1, 20: 3}, staff_threshold=4)
    REGISTRY.register("CANDIDA ALBICANS, PRESUMPTIVE IDENTIFICATION", org_id=None, danger_weight=1.0, time_window_days=2, ward_thresholds={5: 1, 10: 1, 20: 2}, staff_threshold=3)
    REGISTRY.register("STREPTOCOCCUS SPECIES", org_id=None, danger_weight=1.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=4)
    REGISTRY.register("STREPTOCOCCUS PNEUMONIAE", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ENTEROCOCCUS FAECALIS", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ENTEROCOCCUS FAECIUM", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ENTEROCOCCUS SP.", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("PROBABLE ENTEROCOCCUS", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("ALPHA STREPTOCOCCI", org_id=None, danger_weight=1.0, time_window_days=2, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=4)
    REGISTRY.register("BETA STREPTOCOCCUS GROUP B", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("PRESUMPTIVE STREPTOCOCCUS BOVIS", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 2, 20: 3}, staff_threshold=3)
    REGISTRY.register("GRAM POSITIVE COCCUS(COCCI)", org_id=None, danger_weight=1.0, time_window_days=2, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=4)
    REGISTRY.register("GRAM POSITIVE RODS", org_id=None, danger_weight=1.5, time_window_days=2, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=3)
    REGISTRY.register("CORYNEBACTERIUM SPECIES (DIPHTHEROIDS)", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("CORYNEBACTERIUM STRIATUM", org_id=None, danger_weight=2.0, time_window_days=2, ward_thresholds={5: 1, 10: 2, 20: 3}, staff_threshold=2)
    REGISTRY.register("LACTOBACILLUS SPECIES", org_id=None, danger_weight=1.0, time_window_days=3, ward_thresholds={5: 3, 10: 4, 20: 5}, staff_threshold=4)
    REGISTRY.register("MYCOBACTERIUM AVIUM COMPLEX", org_id=None, danger_weight=1.5, time_window_days=1, ward_thresholds={5: 1, 10: 2, 20: 4}, staff_threshold=3)
    REGISTRY.register("GRAM POSITIVE BACTERIA", org_id=None, danger_weight=1.0, time_window_days=2, ward_thresholds={5: 2, 10: 3, 20: 4}, staff_threshold=4)

    # GENERIC / LOW-SIGNAL
    REGISTRY.register("ORGANISM", org_id=None, danger_weight=0.5, time_window_days=7, ward_thresholds={5: 5, 10: 7, 20: 10}, staff_threshold=5)

    return REGISTRY
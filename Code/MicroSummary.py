# micro_analysis.py
"""
MicroAnalysis — ward/unit outbreak detection with intelligent per-pathogen thresholds.

If a ThresholdEngine is provided, each organism gets its own data-driven threshold.
If not, falls back to the flat self.threshold for backwards compatibility.
"""

from __future__ import annotations
import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Dict, Optional


# ─── Threshold Engine ─────────────────────────────────────────────────────────

@dataclass
class PathogenThresholdProfile:
    org_name: str
    base_count: int
    prevalence_rank: int
    danger_weight: float
    computed_threshold: int
    rationale: str


class ThresholdEngine:
    """
    Computes per-pathogen alert thresholds from MIMIC data + clinical danger weights.

    Formula
    -------
        threshold = max(MIN_FLOOR, round(p80_ward_count / danger_weight))

    - p80_ward_count : 80th percentile of positive-test counts across wards
                       for this organism (organism-specific if ≥5 wards,
                       else falls back to global p80)
    - danger_weight  : clinical multiplier — higher = threshold decreases,
                       so rare/dangerous pathogens alert earlier
    - MIN_FLOOR      : absolute minimum so threshold is never trivially 1
    """

    # Clinically validated danger weights for organisms found in MIMIC-III
    # Prevalence order from MIMIC VAP literature (Liu et al. 2019 Frontiers)
    DANGER_WEIGHTS: Dict[str, float] = {
        # ── Critical (MDR, spore-forming, contact precautions) ────────────────
        "CLOSTRIDIUM DIFFICILE":              3.0,
        "ENTEROCOCCUS FAECIUM":               3.0,   # VRE, 73% resistant in MIMIC
        "ACINETOBACTER BAUMANNII":            3.0,   # pan-resistant strains
        "STAPHYLOCOCCUS AUREUS":              2.5,   # MRSA risk, #2 MIMIC VAP
        "PSEUDOMONAS AERUGINOSA":             2.5,   # MDR, #5 MIMIC VAP
        "KLEBSIELLA PNEUMONIAE":              2.5,   # KPC carbapenemase
        "ENTEROCOCCUS FAECALIS":              2.5,   # VRE risk
        # ── Moderate ─────────────────────────────────────────────────────────
        "CANDIDA GLABRATA":                   1.8,   # fluconazole-resistant
        "GRAM NEGATIVE ROD":                  1.5,   # catch-all gram-neg
        "STAPHYLOCOCCUS COAGULASE NEGATIVE":  1.5,   # device-associated
        "ESCHERICHIA COLI":                   1.5,   # common but treatable
        # ── Low (high prevalence, low transmission risk) ──────────────────────
        "YEAST":                              1.2,   # #1 MIMIC VAP (16.7%)
        "CANDIDA ALBICANS":                   1.2,
        "STREPTOCOCCUS":                      1.2,
        "HAEMOPHILUS INFLUENZAE":             1.0,
    }
    DEFAULT_DANGER_WEIGHT = 1.0
    MIN_FLOOR = 2
    PERCENTILE_CUTOFF = 80

    def __init__(self, micro_merged: pd.DataFrame, ward_col: str = "FIRST_WARDID"):
        df = micro_merged.copy()
        df["_ORG_UPPER"] = df["ORG_NAME"].str.upper().str.strip()
        self._df = df
        self._ward_col = ward_col
        self.profiles: Dict[str, PathogenThresholdProfile] = {}

    def compute(self) -> "ThresholdEngine":
        """Compute threshold profiles for every organism in the data. Returns self for chaining."""
        ward_counts = (
            self._df
            .dropna(subset=["_ORG_UPPER", self._ward_col])
            .groupby(["_ORG_UPPER", self._ward_col])
            .size()
            .reset_index(name="count")
        )
        global_p80 = np.percentile(ward_counts["count"], self.PERCENTILE_CUTOFF)

        organism_totals = (
            self._df
            .dropna(subset=["_ORG_UPPER"])
            .groupby("_ORG_UPPER")
            .size()
            .sort_values(ascending=False)
            .reset_index(name="total")
        )

        for rank, row in enumerate(organism_totals.itertuples(), start=1):
            org = row._ORG_UPPER
            org_wards = ward_counts[ward_counts["_ORG_UPPER"] == org]["count"]

            p80 = (
                np.percentile(org_wards, self.PERCENTILE_CUTOFF)
                if len(org_wards) >= 5
                else global_p80
            )

            danger = self.DANGER_WEIGHTS.get(org, self.DEFAULT_DANGER_WEIGHT)
            raw = p80 / danger
            final = max(self.MIN_FLOOR, round(raw))

            self.profiles[org] = PathogenThresholdProfile(
                org_name=org,
                base_count=int(row.total),
                prevalence_rank=rank,
                danger_weight=danger,
                computed_threshold=final,
                rationale=f"p80={p80:.1f}, danger={danger}, raw={raw:.1f} → threshold={final}",
            )
        return self

    def get_threshold(self, org_name: str) -> int:
        """Return computed threshold for org_name (case-insensitive). Fallback = 5."""
        if not org_name or (isinstance(org_name, float) and np.isnan(org_name)):
            return 5
        key = str(org_name).upper().strip()
        profile = self.profiles.get(key)
        return profile.computed_threshold if profile else 5

    def to_dataframe(self) -> pd.DataFrame:
        """Export all profiles as a DataFrame for inspection."""
        return pd.DataFrame([
            {
                "ORG_NAME": p.org_name,
                "PREVALENCE_RANK": p.prevalence_rank,
                "TOTAL_POSITIVES": p.base_count,
                "DANGER_WEIGHT": p.danger_weight,
                "ALERT_THRESHOLD": p.computed_threshold,
                "RATIONALE": p.rationale,
            }
            for p in sorted(self.profiles.values(), key=lambda x: x.prevalence_rank)
        ])

    def print_report(self) -> None:
        if not self.profiles:
            print("Run .compute() first.")
            return
        print(f"\n{'─'*72}")
        print("  Per-Pathogen Alert Thresholds (ThresholdEngine)")
        print(f"{'─'*72}")
        print(f"  {'Organism':<42} {'Rank':>4}  {'Total':>6}  {'Danger':>6}  {'Alert@':>6}")
        print(f"{'─'*72}")
        for p in sorted(self.profiles.values(), key=lambda x: x.prevalence_rank):
            print(
                f"  {p.org_name:<42} {p.prevalence_rank:>4}  "
                f"{p.base_count:>6}  {p.danger_weight:>6.1f}  "
                f"{p.computed_threshold:>6}"
            )
        print(f"{'─'*72}")


# ─── MicroAnalysis ────────────────────────────────────────────────────────────

class MicroAnalysis:
    """
    Microbiology threshold calculations and unit/ward outbreak alerts.

    Parameters
    ----------
    threshold       : flat fallback threshold used when no ThresholdEngine is given
    ward_threshold  : number of affected wards in a unit before unit-level alert fires
    engine          : optional ThresholdEngine — when provided, each organism
                      gets its own data-driven threshold instead of `threshold`
    """

    def __init__(
        self,
        threshold: int = 5,
        ward_threshold: int = 2,
        engine: Optional[ThresholdEngine] = None,
    ):
        self.threshold = threshold
        self.ward_threshold = ward_threshold
        self.engine = engine
        self.ward_spec: Optional[pd.DataFrame] = None
        self.unit_summary: Optional[pd.DataFrame] = None

    # ── Core analysis ─────────────────────────────────────────────────────────

    def analyze_ward_specimens(self, micro_merged: pd.DataFrame) -> pd.DataFrame:
        """
        Count positive tests per ward/specimen/organism; flag rows exceeding threshold.

        When a ThresholdEngine is attached, each organism's threshold is looked up
        individually. The THRESHOLD column in the output shows which value was used.
        """
        ward_spec = (
            micro_merged
            .groupby(["FIRST_WARDID", "SPEC_ITEMID", "ORG_NAME", "FIRST_CAREUNIT"])
            .size()
            .reset_index(name="positive_tests")
        )
        ward_spec.columns = [
            "WARD_ID", "SPECIMEN_ID", "SPECIMEN_NAME", "CARE_UNIT", "POSITIVE_TESTS"
        ]

        # ── Apply threshold: per-organism if engine present, else flat ─────
        if self.engine is not None:
            ward_spec["THRESHOLD"] = ward_spec["SPECIMEN_NAME"].map(
                self.engine.get_threshold
            )
        else:
            ward_spec["THRESHOLD"] = self.threshold

        ward_spec["ALERT"] = ward_spec["POSITIVE_TESTS"] > ward_spec["THRESHOLD"]
        ward_spec = ward_spec.sort_values("POSITIVE_TESTS", ascending=False)
        ward_spec = ward_spec[[
            "WARD_ID", "SPECIMEN_ID", "POSITIVE_TESTS",
            "SPECIMEN_NAME", "THRESHOLD", "ALERT", "CARE_UNIT"
        ]]

        self.ward_spec = ward_spec
        return ward_spec

    def analyze_unit_wards(self) -> pd.DataFrame:
        """Count distinct wards per unit that have at least one ALERT row."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first.")

        # Only count wards that actually breached their threshold
        alerted_rows = self.ward_spec[self.ward_spec["ALERT"]]
        unit_summary = (
            alerted_rows
            .groupby("CARE_UNIT")["WARD_ID"]
            .nunique()
            .reset_index(name="affected_wards")
        )
        unit_summary["UNIT_ALERT"] = unit_summary["affected_wards"] > self.ward_threshold
        self.unit_summary = unit_summary
        return unit_summary

    # ── Reporting ─────────────────────────────────────────────────────────────

    def print_ward_reports(self) -> None:
        """Print alerts and full ward-specimen summary."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first.")

        mode = "per-organism (ThresholdEngine)" if self.engine else f"flat={self.threshold}"
        print(f"\nMICROBIOLOGY POSITIVE TESTS BY WARD AND SPECIMEN  [threshold mode: {mode}]")
        print("─" * 90)

        alerts = self.ward_spec[self.ward_spec["ALERT"]]
        print(f"\nALERTS — rows exceeding their organism threshold:")
        print("─" * 90)
        print(alerts.to_string(index=False) if len(alerts) > 0 else "  No alerts.")

        print("\n\nComplete Summary (all ward-specimen combinations):")
        print("─" * 90)
        print(self.ward_spec.to_string(index=False))

    def print_unit_alerts(self) -> None:
        """Print unit-level alerts (units with > ward_threshold alerted wards)."""
        if self.ward_spec is None:
            raise ValueError("Run analyze_ward_specimens() first.")
        if self.unit_summary is None:
            self.analyze_unit_wards()

        print(f"\nALERT: UNITS WITH MORE THAN {self.ward_threshold} AFFECTED WARDS")
        print("─" * 90)

        unit_alerts = self.unit_summary[self.unit_summary["UNIT_ALERT"]]

        if len(unit_alerts) == 0:
            print(f"\n  ✓ No unit-level alerts: all units have ≤ {self.ward_threshold} affected wards.")
            return

        for _, row in unit_alerts.iterrows():
            unit = row["CARE_UNIT"]
            num_wards = row["affected_wards"]
            unit_data = self.ward_spec[self.ward_spec["CARE_UNIT"] == unit]
            affected_ward_ids = sorted(
                [int(w) for w in unit_data["WARD_ID"].unique() if pd.notna(w)]
            )

            print(f"\n  UNIT : {unit}")
            print(f"  Affected wards : {num_wards} → {affected_ward_ids}")
            print(f"  Specimens detected:")

            for _, spec_row in unit_data.sort_values("POSITIVE_TESTS", ascending=False).iterrows():
                flag = "⚠" if spec_row["ALERT"] else " "
                print(
                    f"    {flag} Ward {int(spec_row['WARD_ID'])}: "
                    f"{int(spec_row['POSITIVE_TESTS'])} positives of "
                    f"'{spec_row['SPECIMEN_NAME']}' "
                    f"(threshold={int(spec_row['THRESHOLD'])})"
                )

    def get_alerts(self) -> pd.DataFrame:
        """Return only rows where ALERT=True."""
        return (
            self.ward_spec[self.ward_spec["ALERT"]].copy()
            if self.ward_spec is not None
            else pd.DataFrame()
        )

    def get_threshold_summary(self) -> Optional[pd.DataFrame]:
        """
        If an engine is attached, return the full threshold profile table.
        Useful for logging which thresholds were applied in a given run.
        """
        if self.engine is None:
            print("No ThresholdEngine attached. Using flat threshold.")
            return None
        return self.engine.to_dataframe()

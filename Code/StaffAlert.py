# module_4_staff_alert.py
"""
Staff Temporal Exposure Alert
==============================
Monitors caregiver exposure to ICU units that have active microbiology
or pathogen alerts. Raises a StaffTemporalAlert when a staff member has
worked in more than UNIT_THRESHOLD distinct alerted units within a
rolling TIME_WINDOW_DAYS day window.

Dependencies
------------
- models.py         : Staff, Patient, Hospital data classes
- module_2_micro_alert.py  : UnitMicroAlert, MicrobiologyAlertMonitor
- module_3_pathogen_alert.py: PathogenAlert, DangerousPathogenMonitor

Inputs
------
alert_log : list[AlertLogEntry]
    A persistent log of all alerts ever raised (from Modules 2 & 3).
    Append every alert produced by .run() to this list over time.

staff_exposure_log : list[StaffExposureEvent]
    One entry per (staff, patient, icu_unit, time) interaction.
    Build this from CHARTEVENTS.csv during loading:
        for each row: StaffExposureEvent(cgid, subject_id, icu_name, charttime)

Usage
-----
    monitor = StaffTemporalExposureMonitor(
        staff_registry=loader._staff_registry,
        alert_log=global_alert_log,
        staff_exposure_log=global_exposure_log,
        reference_time=datetime.now(),
    )
    alerts = monitor.run()
    monitor.report()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set
from collections import defaultdict

from models import Staff


# Tuneable constants
TIME_WINDOW_DAYS: int = 7       # rolling window length
UNIT_THRESHOLD: int = 3         # alert if staff touched > this many alerted units


# Shared data-transfer objects (also used by Modules 2 & 3) 

@dataclass
class AlertLogEntry:
    """
    A single raised alert stored in the shared alert log.
    Created by MicrobiologyAlertMonitor and DangerousPathogenMonitor
    and appended to a global list that this module reads.

    Attributes
    ----------
    icu_name   : name of the ICU unit where the alert was raised
    alert_type : 'MICRO' | 'PATHOGEN'
    raised_at  : wall-clock time when monitor.run() was called
    detail     : optional human-readable summary (e.g. organism name)
    """
    icu_name: str
    alert_type: str          # 'MICRO' | 'PATHOGEN'
    raised_at: datetime
    detail: Optional[str] = None


@dataclass
class StaffExposureEvent:
    """
    One interaction between a caregiver and a patient in an ICU unit.
    Build from CHARTEVENTS rows during data loading.

    Attributes
    ----------
    cgid       : CAREGIVERS.CGID
    subject_id : patient identifier
    icu_name   : ICUSTAYS.FIRST_CAREUNIT for the stay linked to this event
    event_time : CHARTEVENTS.CHARTTIME
    """
    cgid: int
    subject_id: int
    icu_name: str
    event_time: datetime


# ─── Alert output dataclass ───────────────────────────────────────────────────

@dataclass
class StaffTemporalAlert:
    """
    Fired when a caregiver's exposure to alerted units exceeds UNIT_THRESHOLD
    within the rolling time window.

    Attributes
    ----------
    cgid                  : caregiver identifier
    label                 : caregiver role label (e.g. 'RN', 'MD')
    alerted_units_exposed : distinct ICU unit names where alerts were active
                            AND the staff member worked, within the window
    earliest_exposure     : timestamp of the oldest qualifying exposure event
    latest_exposure       : timestamp of the most recent qualifying exposure event
    window_start          : start of the rolling window used for this check
    """
    cgid: int
    label: Optional[str]
    alerted_units_exposed: List[str]
    earliest_exposure: datetime
    latest_exposure: datetime
    window_start: datetime

    @property
    def unit_count(self) -> int:
        return len(self.alerted_units_exposed)

    @property
    def message(self) -> str:
        return (
            f"[STAFF TEMPORAL ALERT] "
            f"CGID={self.cgid} ({self.label or 'Unknown role'}) | "
            f"Exposed to {self.unit_count} alerted units in past "
            f"{TIME_WINDOW_DAYS}d (threshold={UNIT_THRESHOLD}) | "
            f"Units: {', '.join(sorted(self.alerted_units_exposed))} | "
            f"Window: {self.window_start.date()} → {self.latest_exposure.date()}"
        )


# ─── Main monitor class ───────────────────────────────────────────────────────

class StaffTemporalExposureMonitor:
    """
    Scans the alert log and staff exposure log over a rolling window
    and fires StaffTemporalAlert for any caregiver with too many
    distinct alerted-unit contacts.

    Parameters
    ----------
    staff_registry      : dict mapping cgid → Staff object
    alert_log           : shared list of AlertLogEntry (append-only, from M2/M3)
    staff_exposure_log  : list of StaffExposureEvent (from CHARTEVENTS loader)
    reference_time      : the "now" anchor for the rolling window;
                          defaults to datetime.utcnow() if not provided
    time_window_days    : length of the rolling window (default 7)
    unit_threshold      : fire alert when distinct alerted units > this (default 3)
    """

    def __init__(
        self,
        staff_registry: Dict[int, Staff],
        alert_log: List[AlertLogEntry],
        staff_exposure_log: List[StaffExposureEvent],
        reference_time: Optional[datetime] = None,
        time_window_days: int = TIME_WINDOW_DAYS,
        unit_threshold: int = UNIT_THRESHOLD,
    ):
        self.staff_registry = staff_registry
        self.alert_log = alert_log
        self.staff_exposure_log = staff_exposure_log
        self.reference_time = reference_time or datetime.utcnow()
        self.time_window_days = time_window_days
        self.unit_threshold = unit_threshold
        self.alerts: List[StaffTemporalAlert] = []

    # ── Public interface ──────────────────────────────────────────────────────

    def run(self) -> List[StaffTemporalAlert]:
        """Execute the full scan and return all raised alerts."""
        self.alerts = []
        window_start = self.reference_time - timedelta(days=self.time_window_days)

        alerted_units = self._alerted_units_in_window(window_start)
        if not alerted_units:
            return self.alerts  # nothing to check against

        exposure_index = self._build_exposure_index(window_start, alerted_units)

        for cgid, unit_events in exposure_index.items():
            # unit_events: dict[icu_name -> list[event_time]]
            if len(unit_events) > self.unit_threshold:
                all_times = [t for times in unit_events.values() for t in times]
                staff = self.staff_registry.get(cgid)
                self.alerts.append(
                    StaffTemporalAlert(
                        cgid=cgid,
                        label=staff.label if staff else None,
                        alerted_units_exposed=list(unit_events.keys()),
                        earliest_exposure=min(all_times),
                        latest_exposure=max(all_times),
                        window_start=window_start,
                    )
                )
        return self.alerts

    def report(self) -> None:
        """Print all alerts to stdout."""
        if not self.alerts:
            print(
                f"No staff exposure alerts "
                f"(window={self.time_window_days}d, threshold={self.unit_threshold} units)."
            )
            return
        print(f"{'─'*70}")
        print(f"  STAFF TEMPORAL EXPOSURE REPORT  |  {len(self.alerts)} alert(s) raised")
        print(f"  Reference time : {self.reference_time.isoformat()}")
        print(f"  Window         : {self.time_window_days} days")
        print(f"  Unit threshold : > {self.unit_threshold} alerted units")
        print(f"{'─'*70}")
        for alert in sorted(self.alerts, key=lambda a: a.unit_count, reverse=True):
            print(alert.message)
        print(f"{'─'*70}")

    def get_summary_table(self) -> List[Dict]:
        """
        Return alerts as a list of plain dicts for DataFrame conversion.

        Example
        -------
            import pandas as pd
            df = pd.DataFrame(monitor.get_summary_table())
        """
        return [
            {
                "cgid": a.cgid,
                "label": a.label,
                "alerted_units_count": a.unit_count,
                "alerted_units": ", ".join(sorted(a.alerted_units_exposed)),
                "earliest_exposure": a.earliest_exposure,
                "latest_exposure": a.latest_exposure,
                "window_start": a.window_start,
            }
            for a in self.alerts
        ]

    # ── Private helpers ───────────────────────────────────────────────────────

    def _alerted_units_in_window(self, window_start: datetime) -> Set[str]:
        """
        Return the set of ICU unit names that had ANY alert raised
        inside [window_start, reference_time].
        """
        return {
            entry.icu_name
            for entry in self.alert_log
            if window_start <= entry.raised_at <= self.reference_time
        }

    def _build_exposure_index(
        self,
        window_start: datetime,
        alerted_units: Set[str],
    ) -> Dict[int, Dict[str, List[datetime]]]:
        """
        Build:  cgid → { icu_name → [event_time, ...] }

        Only includes exposure events that are:
          1. Within [window_start, reference_time]
          2. In a unit that had an active alert in the same window
        """
        index: Dict[int, Dict[str, List[datetime]]] = defaultdict(lambda: defaultdict(list))

        for event in self.staff_exposure_log:
            in_window = window_start <= event.event_time <= self.reference_time
            in_alerted_unit = event.icu_name in alerted_units
            if in_window and in_alerted_unit:
                index[event.cgid][event.icu_name].append(event.event_time)

        return index

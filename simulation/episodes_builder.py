from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Set
import pandas as pd
from microbiology.pathogens import PathogenRegistry, Pathogen


@dataclass
class CultureEpisode:

    """
    Represents a microbiology episode within a ward.

    Tracks patients, time window, and whether alert thresholds are met.
    """

    episode_id: int
    ward_id: int
    ward_size: int
    pathogen: Pathogen
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    patient_ids: Set[int] = field(default_factory=set)
    culture_events: int = 0
    window_days: int = 0
    threshold: int = 0
    alert: bool = False

    @property
    def org_name(self) -> str:
        return self.pathogen.key

    @property
    def unique_patients(self) -> int:
        return len(self.patient_ids)

    def should_alert(self) -> bool:
        return self.unique_patients >= self.threshold

    def to_dict(self) -> dict:
        return {
            "EPISODE_ID": self.episode_id,
            "WARD_ID": self.ward_id,
            "ORG_NAME": self.org_name,
            "NUM_PATIENTS": self.unique_patients,
            "START_TIME": self.start_time,
            "END_TIME": self.end_time,
            "CULTURE_EVENTS": self.culture_events,
            "THRESHOLD": self.threshold,
            "WINDOW_DAYS": self.window_days,
            "ALERT": self.alert,
        }


class CultureEpisodeBuilder:

    """
    Incrementally builds a CultureEpisode from event rows.

    Aggregates time range, patients, and event counts.
    """

    def __init__(self, episode_id: int, ward_id: int, ward_size: int, pathogen: Pathogen):
        self.episode_id = episode_id
        self.ward_id = ward_id
        self.ward_size = ward_size
        self.pathogen = pathogen
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.patient_ids: Set[int] = set()
        self.culture_events = 0

    def add_event(self, row: pd.Series) -> None:
        chartdate = row["CHARTDATE"]
        outtime = row["OUTTIME"]

        if pd.notna(chartdate) and (self.start_time is None or chartdate < self.start_time):
            self.start_time = chartdate
        if pd.notna(outtime) and (self.end_time is None or outtime > self.end_time):
            self.end_time = outtime

        self.culture_events += 1
        if pd.notna(row["SUBJECT_ID"]):
            self.patient_ids.add(int(row["SUBJECT_ID"]))

    def build(self) -> CultureEpisode:
        threshold = self.pathogen.get_ward_threshold(self.ward_size)
        alert = len(self.patient_ids) >= threshold
        return CultureEpisode(
            episode_id=self.episode_id,
            ward_id=self.ward_id,
            ward_size=self.ward_size,
            pathogen=self.pathogen,
            start_time=self.start_time,
            end_time=self.end_time,
            patient_ids=self.patient_ids,
            culture_events=self.culture_events,
            window_days=self.pathogen.time_window_days,
            threshold=threshold,
            alert=alert,
        )


class EpisodesBuilder:

    """
    Generates CultureEpisode objects from ward-level microbiology data.

    Groups events by pathogen and ward, and splits episodes based on
    time gaps and occupancy.
    """

    def __init__(self, registry: PathogenRegistry):
        self.registry = registry

    def generate(self, ward_pos_all: pd.DataFrame) -> List[CultureEpisode]:
        episodes: List[CultureEpisode] = []

        for pathogen in self.registry.all_pathogens():
            org_name = pathogen.key
            ward_data = ward_pos_all[ward_pos_all["ORG_NAME"] == org_name].copy()
            if ward_data.empty:
                continue

            for ward_id, ward_group in ward_data.groupby("WARD_ID"):
                df = ward_group.sort_values("CHARTDATE").reset_index(drop=True)
                ward_size = int(ward_group["WARD_SIZE"].iloc[0])
                episode_id = 0
                builder: Optional[CultureEpisodeBuilder] = None

                for _, row in df.iterrows():
                    if builder is None:
                        builder = CultureEpisodeBuilder(episode_id, ward_id, ward_size, pathogen)
                        builder.add_event(row)
                        continue

                    still_occupied = (
                        pd.notna(builder.end_time) and pd.notna(row["CHARTDATE"]) and row["CHARTDATE"] <= builder.end_time
                    )
                    gap = (
                        (row["CHARTDATE"] - builder.start_time).total_seconds() / 86400
                        if pd.notna(row["CHARTDATE"]) and pd.notna(builder.start_time)
                        else float("inf")
                    )

                    if not still_occupied and gap > pathogen.time_window_days:
                        episodes.append(builder.build())
                        episode_id += 1
                        builder = CultureEpisodeBuilder(episode_id, ward_id, ward_size, pathogen)

                    if builder is None:
                        builder = CultureEpisodeBuilder(episode_id, ward_id, ward_size, pathogen)
                    builder.add_event(row)

                if builder is not None:
                    episodes.append(builder.build())

        print(f"Generated {len(episodes)} episodes across {len({ep.ward_id for ep in episodes})} wards")
        return episodes

"""This file is responsible for connecting the extracted, clean data frames with created microbiology alert classes into 1"""

from microbiology.alerts import MicrobiologyAlert
from microbiology.patients import Patient
from microbiology.wards import Ward



class AlertGenerator:
    def __init__(self, registry):
        self.registry = registry
        self.counter_id = 0
        self.wards = {}  # ward_id → Ward

    def generate(self, episodes_df):
        alerts = []

        for _, episode in episodes_df.iterrows():
            pathogen = self.registry.get(episode["org_name"])
            if not pathogen:
                continue

            ward_id = int(episode["ward_id"])

            # --- get or create Ward ---
            if ward_id not in self.wards:
                self.wards[ward_id] = Ward(
                    ward_id=ward_id,
                    ward_size=int(episode["ward_size"])
                )

            ward = self.wards[ward_id]

            # --- create and assign patient ---
            patient = self._create_patient(episode, pathogen)
            ward.add_patient(patient)

            # --- decision: Ward decides outbreak ---
            if ward.has_outbreak(pathogen):
                alert = self._create_alert(episode, pathogen, ward)
                alerts.append(alert)
                self.counter_id += 1

        return alerts, self._collect_all_patients()

    def _create_alert(self, episode, pathogen, ward):
        return MicrobiologyAlert(
            counter_id=self.counter_id,
            pathogen=pathogen,
            ward_id=ward.ward_id,
            ward_size=ward.ward_size,
            start_time=episode["start_time"],
            alert_type="WARD",
            curr_patient_number=len(
                ward.get_patients_by_pathogen(pathogen)
            )
        )

    def _create_patient(self, episode, pathogen):
        return Patient(
            patient_id=int(episode["patient_id"]),
            ward_id=int(episode["ward_id"]),
            pathogen_name=pathogen
        )
    
    def _collect_all_patients(self):
        all_patients = []
        for ward in self.wards.values():
            all_patients.extend(ward.patients)
        return all_patients
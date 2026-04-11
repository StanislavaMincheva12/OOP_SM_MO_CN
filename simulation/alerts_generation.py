from microbiology.alerts import MicrobiologyAlert

class AlertGenerator:

    """
    Generates microbiology alerts from episode data.

    Uses a pathogen registry to evaluate whether thresholds are exceeded
    and creates alerts accordingly. Supports flexible input formats
    (objects, dicts, DataFrames).
    """

    def __init__(self, registry):
        self.registry = registry
        self.counter_id = 0

    def generate(self, episodes):
        alerts = []

        for episode in episodes:
            org_name = self._get_attribute(episode, "org_name")
            pathogen = self.registry.get(org_name)
            if not pathogen:
                continue

            ward_size = int(self._get_attribute(episode, "ward_size"))
            num_patients = int(self._get_attribute(episode, "unique_patients") or self._get_attribute(episode, "NUM_PATIENTS") or 0)
            threshold = pathogen.get_ward_threshold(ward_size)

            if num_patients >= threshold:
                alert = self._create_alert(episode, pathogen, num_patients)
                alerts.append(alert)
                self.counter_id += 1

        return alerts, []

    def _create_alert(self, episode, pathogen, num_patients):
        return MicrobiologyAlert(
            counter_id=self.counter_id,
            pathogen=pathogen,
            ward_id=int(self._get_attribute(episode, "ward_id") or self._get_attribute(episode, "WARD_ID")),
            ward_size=int(self._get_attribute(episode, "ward_size") or self._get_attribute(episode, "WARD_SIZE")),
            start_time=self._get_attribute(episode, "start_time"),
            alert_type="WARD",
            curr_patient_number=num_patients
        )

    def _get_attribute(self, source, name):
        if hasattr(source, name):
            return getattr(source, name)

        if isinstance(source, dict):
            return source.get(name) or source.get(name.upper())

        try:
            return source[name]
        except Exception:
            pass

        if hasattr(source, "to_dict"):
            return source.to_dict().get(name) or source.to_dict().get(name.upper())

        return None

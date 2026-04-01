class Ward:
    def __init__(self, ward_id, ward_size):
        self.ward_id = ward_id
        self.ward_size = ward_size
        self.patients = []

    def add_patient(self, patient):
        self.patients.append(patient)

    def get_patients_by_pathogen(self, pathogen):
        return [
            p for p in self.patients
            if p.pathogen.key == pathogen.key
        ]

    def has_outbreak(self, pathogen):
        infected = len(self.get_patients_by_pathogen(pathogen))
        threshold = pathogen.get_ward_threshold(self.ward_size)

        return infected >= threshold
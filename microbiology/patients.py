from .pathogens import Pathogen
from dataclasses import dataclass

@dataclass
class Patient():

    patient_id: int
    ward_id: int
    pathogen_name: Pathogen

    @property
    def id(self) -> int:
        return self.patient_id
    
    @property
    def ward(self) -> int:
        return self.ward_id
    
    @property
    def pathogen(self) -> Pathogen:
        return self.pathogen_name
    
    def to_dict(self):
        return {
            "PATIENT_ID": self.patient_id,
            "ORG_NAME": self.pathogen.key,
            "WARD_ID": self.ward_id
        }
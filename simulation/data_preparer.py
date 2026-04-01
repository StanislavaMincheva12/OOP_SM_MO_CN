from database.data_preparation import prepare_microbiology_data as prep_func
from database.data_repository import DataRepository
import pandas as pd

class MicrobiologyDataPreparer:
    """
    Prepares microbiology data from the repository into a format
    suitable for generating episodes and alerts.
    """

    def __init__(self, repository: DataRepository):
        self.repository = repository
        self.ward_pos_all: pd.DataFrame | None = None

    def prepare(self) -> pd.DataFrame:
        """
        Converts raw repository data into ward-patient-pathogen table.
        """
        self.ward_pos_all = prep_func(self.repository)
        return self.ward_pos_all

    @property
    def data(self) -> pd.DataFrame:
        if self.ward_pos_all is None:
            raise RuntimeError("Data not prepared yet. Call .prepare() first.")
        return self.ward_pos_all
from database.db_connector_loader import TableLoader
from database.data_repository import DataRepository
from microbiology.pathogens import load_default_pathogens
from database.data_preparation import prepare_microbiology_data
from simulation.episodes_builder import EpisodesBuilder
from simulation.alerts_generation import AlertGenerator
from simulation.data_preparer import MicrobiologyDataPreparer
from microbiology.wards import Hospital


class HospitalAlertSystem:
    """Façade for running the full microbiology alert pipeline."""

    def __init__(self, db_path: str, tables: list[str]):
        self.db_path = db_path
        self.tables = tables
        self.registry = load_default_pathogens()

    def run(self):
        loader = TableLoader(self.db_path)
        data = loader.load_tables(self.tables)

        repo = DataRepository.from_dict(data)
        data_preparer = MicrobiologyDataPreparer(repo)
        ward_pos_all = data_preparer.prepare()

        ep_builder = EpisodesBuilder(self.registry)
        episodes = ep_builder.generate(ward_pos_all)

        generator = AlertGenerator(self.registry)
        alerts, _ = generator.generate(episodes)

        return alerts

    def build_hospital(self):
        loader = TableLoader(self.db_path)
        data = loader.load_tables(self.tables)

        repo = DataRepository.from_dict(data)
        data_preparer = MicrobiologyDataPreparer(repo)
        ward_pos_all = data_preparer.prepare()

        hospital = Hospital.from_dataframe(ward_pos_all, self.registry)
        return hospital

    def run_and_serialize(self):
        alerts = self.run()
        return [alert.to_dict() for alert in alerts]

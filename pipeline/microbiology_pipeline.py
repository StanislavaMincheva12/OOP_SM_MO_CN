from database.db_connector_loader import TableLoader
from database.data_repository import DataRepository
from microbiology.pathogens import load_default_pathogens
from database.data_preparation import prepare_microbiology_data
from simulation.episodes_builder import EpisodesBuilder
from simulation.alerts_generation import AlertGenerator
from simulation.data_preparer import MicrobiologyDataPreparer
import pandas as pd


class MicrobiologyPipeline:
    def __init__(self, db_path, tables):
        self.db_path = db_path
        self.tables = tables

    def run(self):
        loader = TableLoader(self.db_path)
        data = loader.load_tables(self.tables)

        repo = DataRepository.from_dict(data)
        registry = load_default_pathogens()

        data_preparer = MicrobiologyDataPreparer(repo)
        ward_pos_all = data_preparer.prepare()

        ep_builder = EpisodesBuilder(registry)
        episodes = ep_builder.generate(ward_pos_all)

        generator = AlertGenerator(registry)
        alerts, _ = generator.generate(episodes)

        alerts_df = pd.DataFrame([a.to_dict() for a in alerts])
        return alerts_df

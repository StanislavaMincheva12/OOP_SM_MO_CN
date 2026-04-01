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
        # Load data using loader
        loader = TableLoader(self.db_path)
        data = loader.load_tables(self.tables)

        # Use repo
        repo = DataRepository.from_dict(data)
        registry = load_default_pathogens()

        # Use Preparer object to make data preparations
        data_preparer = MicrobiologyDataPreparer(repo)
        ward_pos_all = data_preparer.prepare()
        
        # Use episodes builder to make episodes from microbiological data
        ep_builder = EpisodesBuilder(registry)
        episodes_df = ep_builder.generate(ward_pos_all)

        # Use alerts generator to generate alerts from episodes
        generator = AlertGenerator(registry)
        alerts, patients = generator.generate(episodes_df)

        alerts_df = pd.DataFrame([a.to_dict() for a in alerts])

        return alerts_df
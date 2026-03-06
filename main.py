from database.db_loader import load_tables
from database.data_repository import DataRepository

db_path = "OOP_database.db" # db with no alerts yet (downloaded from telegram)

tables = [
        "PATIENTS",
        "CAREGIVERS",
        "D_ITEMS",
        "ADMISSIONS",
        "ICUSTAYS",
        "NOTEEVENTS",
        "MICROBIOLOGYEVENTS",
        "TRANSFERS",
        "OUTPUTEVENTS",
        "CHARTEVENTS",
    ]

# 1) Load data
data = load_tables(db_path, tables)

# 2) Repository
repo = DataRepository.from_dict(data)

# Example of calling a data table using repo

# instead of patiets = data['Patients'], just:

print('Patients table:', repo.patients)








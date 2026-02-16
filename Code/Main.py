import pandas as pd
import matplotlib.pyplot as plt

"""Loading the data and transforming main merges, capitalization of table names etc."""

callout = pd.read_csv("CALLOUT.csv")
admissions = pd.read_csv("ADMISSIONS.csv")
caregivers = pd.read_csv("CAREGIVERS.csv")
chartevents = pd.read_csv("CHARTEVENTS.csv")
cpt_events = pd.read_csv("CPTEVENTS.csv")
d_cpt = pd.read_csv("D_CPT.csv")
d_icd_diagnoses = pd.read_csv("D_ICD_DIAGNOSES.csv") 
d_icd_procedures = pd.read_csv("D_ICD_PROCEDURES.csv")
d_items = pd.read_csv("D_ITEMS.csv")
diagnoses_icd = pd.read_csv("DIAGNOSES_ICD.csv") 
drgcodes = pd.read_csv("DRGCODES.csv") 
icustays = pd.read_csv("ICUSTAYS.csv") 
inputevents_mv = pd.read_csv("INPUTEVENTS_MV.csv") 
inputevents_cv = pd.read_csv("INPUTEVENTS_CV.csv") 
labevents = pd.read_csv("LABEVENTS.csv") 
microbiologyevents = pd.read_csv("MICROBIOLOGYEVENTS.csv") 
noteevents = pd.read_csv("NOTEEVENTS.csv") 
outputevents = pd.read_csv("OUTPUTEVENTS.csv") 
prescriptions = pd.read_csv("PRESCRIPTIONS.csv") 
procedures_icd = pd.read_csv("PROCEDURES_ICD.csv") 
procedures_events_mv= pd.read_csv("PROCEDUREEVENTS_MV.csv") 
services = pd.read_csv("SERVICES.csv") 
transfers = pd.read_csv("TRANSFERS.csv")
patients = pd.read_csv("PATIENTS.csv")

"""For microbiology we need to merge ICU and Microbiology"""
# Capitalize column names
microbiologyevents.columns = microbiologyevents.columns.str.upper()
icustays.columns = icustays.columns.str.upper()

# Merge microbiology with ICU stays to get ward and unit info
micro_merged = microbiologyevents.merge(
    icustays[['HADM_ID', 'FIRST_WARDID', 'FIRST_CAREUNIT']],
    on='HADM_ID',
    how='left'
)

from MicroSummary import MicroAnalysis

analyzer = MicroAnalysis(threshold=5, ward_threshold=2)
analyzer.analyze_ward_specimens(micro_merged)
analyzer.print_ward_reports()
analyzer.print_unit_alerts()

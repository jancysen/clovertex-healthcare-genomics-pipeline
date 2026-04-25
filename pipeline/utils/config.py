import os
from pathlib import Path

# Base Paths (Relative to the project root which should be `main.py` execution directory)
BASE_DIR = Path(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
DATA_DIR = BASE_DIR / "data"
REFERENCE_DIR = DATA_DIR / "reference"
DATALAKE_DIR = BASE_DIR / "datalake"

# Datalake layers
RAW_DIR = DATALAKE_DIR / "raw"
REFINED_DIR = DATALAKE_DIR / "refined"
CONSUMPTION_DIR = DATALAKE_DIR / "consumption"
PLOTS_DIR = CONSUMPTION_DIR / "plots"

# Input data files
CSV_PATIENTS = DATA_DIR / "site_alpha_patients.csv"
JSON_PATIENTS = DATA_DIR / "site_beta_patients.json"
PARQUET_LABS = DATA_DIR / "site_gamma_lab_results.parquet"
CSV_DIAGNOSES = DATA_DIR / "diagnoses_icd10.csv"
JSON_MEDS = DATA_DIR / "medications_log.json"
CSV_NOTES_META = DATA_DIR / "clinical_notes_metadata.csv"
PARQUET_GENOMICS = DATA_DIR / "genomics_variants.parquet"

# Reference files
REF_ICD10 = REFERENCE_DIR / "icd10_chapters.csv"
REF_LAB_RANGES = REFERENCE_DIR / "lab_test_ranges.json"
REF_GENE_REF = REFERENCE_DIR / "gene_reference.json"

# Genomics Filtering Thresholds
GENOMICS_MIN_READ_DEPTH = 20
GENOMICS_MIN_QUALITY_SCORE = 30
GENOMICS_VALID_SIGNIFICANCE = ["Pathogenic", "Likely Pathogenic", "Uncertain Significance"]

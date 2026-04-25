# Clovertex Data Engineering Pipeline

A complete, production-ready, idempotent data engineering pipeline strictly addressing the healthcare + genomics requirements.

## 1. Setup Instructions

To run the pipeline locally using Docker:

1. Clone the repository natively.
2. Place all actual requested input files and reference tables into the `data/` and `data/reference/` directories.
3. Run the following command from the root of the project:
   ```bash
   docker-compose up --build
   ```
4. Wait for execution completion. Check the console for identical JSON logging structure.
5. All outputs, analytics, and plots persist locally via Docker volumes into the `datalake/` directory and `data_quality_report.json`.

## 2. Architecture

**Ingestion & Loaders:**
Pandas and PyArrow read from multiple formats, automatically resolving lines vs blocks in JSON parsing.

**Cleaning & Filtering Modules:**
Specialized Python modules target specific tables. This structure guarantees modularity. You can update cleaning thresholds in `pipeline/utils/config.py`.

**Transformation (Unification):**
All dimensions join onto the patient entity via `pipeline/transformation/unify_data.py`. Summarized arrays/metadata form `patients_master.parquet`.

## 3. Cleaning Decisions

- **Demographics:** Standardized unknown genders, imputed unknown ages to the median. We enforced `patient_id` tracking as string indexes.
- **Null Values:** Avoided dropping whole rows unless essential lookup keys (like `patient_id` or `test_type`) were irreparably absent. Clean datasets are propagated onward.
- **Orphan Entities:** Clinical tables (Labs, Meds, Diagnoses, Genomics) filter sequentially against the verified `patient_id` pool after patient demographics are assessed. Unmatched records are jettisoned and logged under `orphans_removed`.

## 4. Partitioning Rationale

Lab results are strictly partitioned under:
`datalake/refined/lab_results/test_type=.../year=.../month=...`

**Why?**
1. Longitudinal query trends overwhelmingly search by explicit `test_type` first.
2. Slicing chronologically by `year` and `month` protects querying nodes from memory overflows as tables swell across deep time-series healthcare telemetry.

## 5. Anomaly Logic

Six explainable rules are encapsulated inside `pipeline/analytics/anomaly_detection.py` covering:
1. Impossible Ages: Checking outside bounds of 0-120.
2. Impossible Diagnosis Matrix: i.e. Pediatric Prostate Cancer (Age < 18 combined with C61 codes).
3. Dangerous Medications: Tracking cross-reactions (e.g. Warfarin + Aspirin string matches).
4. Unrealistic Labs: Potassium > 10.0 or < 1.0 (deadly combinations).
5. Chromosomal Breaks: Checking for female records presenting Y-chromosome mutations.
6. Missing Clinical Coverage: Verified patients totally missing lab, med, and diagnosed arrays.

## 6. Genomics Filtering Logic

Managed dynamically via `utils/config.py`:
- `GENOMICS_MIN_READ_DEPTH = 20`
- `GENOMICS_MIN_QUALITY_SCORE = 30`
- Configurable clinical significance enums.
Records failing this are categorized as "unreliable_variants_removed".

## 7. Assumptions
- It's assumed the `data/` folder exists and mounts perfectly through the user's host machine.
- Local time formatting is logged as `%Y-%m-%dT%H:%M:%SZ`.

## 8. Future Improvements
- **Airflow Orchestration:** Transition Python explicit execution wrappers to an Airflow DAG context natively mapping dependencies.
- **Delta Lake:** Shift plain PyArrow parquet saves into a robust transactional layer format like Delta Lake natively for vacuum capabilities.
- **Great Expectations:** Enforce tighter programmatic data type contracts.

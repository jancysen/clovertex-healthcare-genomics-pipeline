# Execution Results

## Docker Validation
Pipeline successfully executed using:

docker compose up --build

### Docker Result
- Docker image built successfully
- Container created successfully
- Pipeline executed successfully
- Container exited with code `0`

---

## Data Lake Output Summary

### Raw Layer
Generated manifest:
- 7 source files copied successfully

### Refined Layer
Generated manifest:
- 5 cleaned parquet files generated

Files:
- patients_master.parquet
- labs_cleaned.parquet
- diagnoses_cleaned.parquet
- medications_cleaned.parquet
- genomics_cleaned.parquet

### Consumption Layer
Generated manifest:
- 7 output files generated

Outputs include:
- patient_summary.parquet
- diagnosis_frequency.parquet
- variant_hotspots.parquet
- anomaly_flags.parquet
- visualization outputs
- manifest files

---

## Data Quality Results

### Patient Dataset
- 12 duplicate records removed
- Null values handled
- Invalid dates fixed

### Genomics Dataset
- 553 unreliable variants removed

---

## Visualization Output
Visualization files were successfully generated in:

datalake/consumption/plots/

---

## Final Status

Pipeline Execution Complete ✅  
Docker Validation Complete ✅
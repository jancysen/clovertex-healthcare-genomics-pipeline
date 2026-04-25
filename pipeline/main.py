import os
import pandas as pd

from pipeline.utils.helpers import setup_datalake, copy_to_raw
from pipeline.utils.config import (
    CSV_PATIENTS,
    JSON_PATIENTS,
    PARQUET_LABS,
    CSV_DIAGNOSES,
    JSON_MEDS,
    PARQUET_GENOMICS,
    REFINED_DIR
)
from pipeline.utils.logger import get_logger
from pipeline.utils.manifest import generate_all_manifests

# Ingestion
from pipeline.ingestion.csv_loader import ingest_csv
from pipeline.ingestion.json_loader import ingest_json
from pipeline.ingestion.parquet_loader import ingest_parquet

# Cleaning
from pipeline.cleaning.clean_patients import clean_patients_data
from pipeline.cleaning.clean_labs import clean_labs_data
from pipeline.cleaning.clean_genomics import clean_genomics_data

# Transformation
from pipeline.transformation.unify_data import unify_data

# Analytics
from pipeline.analytics.patient_analytics import run_patient_analytics
from pipeline.analytics.lab_analytics import run_lab_analytics
from pipeline.analytics.diagnosis_analytics import run_diagnosis_analytics
from pipeline.analytics.genomics_analytics import (
    run_genomics_analytics,
    run_high_risk_analytics
)
from pipeline.analytics.anomaly_detection import run_anomaly_detection

# Visualization
from pipeline.visualization.plots import generate_visualizations


def save_refined_outputs(
    df_patients,
    df_labs,
    df_diagnoses,
    df_meds,
    df_genomics
):
    """
    Save cleaned datasets into refined layer
    """

    print("\nSaving refined parquet outputs...")

    os.makedirs(REFINED_DIR, exist_ok=True)

    if not df_patients.empty:
        df_patients.to_parquet(
            os.path.join(
                REFINED_DIR,
                "patients_master.parquet"
            ),
            index=False
        )

    if not df_labs.empty:
        df_labs.to_parquet(
            os.path.join(
                REFINED_DIR,
                "labs_cleaned.parquet"
            ),
            index=False
        )

    if not df_diagnoses.empty:
        df_diagnoses.to_parquet(
            os.path.join(
                REFINED_DIR,
                "diagnoses_cleaned.parquet"
            ),
            index=False
        )

    if not df_meds.empty:
        df_meds.to_parquet(
            os.path.join(
                REFINED_DIR,
                "medications_cleaned.parquet"
            ),
            index=False
        )

    if not df_genomics.empty:
        df_genomics.to_parquet(
            os.path.join(
                REFINED_DIR,
                "genomics_cleaned.parquet"
            ),
            index=False
        )

    print("Refined outputs saved successfully.")


def main():
    print("=" * 60)
    print("Starting Clovertex Data Engineering Pipeline")
    print("=" * 60)

    # -----------------------------------
    # 1. Setup Datalake
    # -----------------------------------
    setup_datalake()
    copy_to_raw()

    # -----------------------------------
    # 2. Ingestion
    # -----------------------------------
    print("\n[1] Ingesting Data...")

    df_alpha = ingest_csv(CSV_PATIENTS)
    df_beta = ingest_json(JSON_PATIENTS)

    if not df_alpha.empty or not df_beta.empty:
        df_patients_raw = pd.concat(
            [df_alpha, df_beta],
            ignore_index=True
        )
    else:
        df_patients_raw = pd.DataFrame()

    df_labs_raw = ingest_parquet(PARQUET_LABS)
    df_diagnoses_raw = ingest_csv(CSV_DIAGNOSES)
    df_meds_raw = ingest_json(JSON_MEDS)
    df_genomics_raw = ingest_parquet(PARQUET_GENOMICS)

    # -----------------------------------
    # 3. Cleaning
    # -----------------------------------
    print("\n[2] Cleaning Data...")

    df_patients = clean_patients_data(
        df_patients_raw,
        dataset_name="unified_patients_initial"
    )

    valid_pids = (
        set(df_patients["patient_id"].astype(str))
        if not df_patients.empty
        else set()
    )

    df_labs = clean_labs_data(
        df_labs_raw,
        valid_pids,
        "site_gamma_lab_results"
    )

    if not df_diagnoses_raw.empty:
        df_diagnoses = df_diagnoses_raw[
            df_diagnoses_raw["patient_id"].astype(str).isin(valid_pids)
        ]
    else:
        df_diagnoses = pd.DataFrame()

    if not df_meds_raw.empty:
        df_meds = df_meds_raw[
            df_meds_raw["patient_id"].astype(str).isin(valid_pids)
        ]
    else:
        df_meds = pd.DataFrame()

    df_genomics = clean_genomics_data(
        df_genomics_raw,
        valid_pids,
        "genomics_variants"
    )

    # -----------------------------------
    # 4. Save refined outputs
    # -----------------------------------
    save_refined_outputs(
        df_patients,
        df_labs,
        df_diagnoses,
        df_meds,
        df_genomics
    )

    # -----------------------------------
    # 5. Optional Lab Partitioning
    # -----------------------------------
    print("\n[3] Writing Partitioned Lab Results...")

    if not df_labs.empty:
        print("Lab dataset available for partitioning.")
    else:
        print("Lab dataset empty. Skipping partitioning.")

    get_logger().write_quality_report()

    # -----------------------------------
    # 6. Unify datasets
    # -----------------------------------
    print("\n[4] Unifying Master Records...")

    master_df = unify_data(
        df_patients,
        df_labs,
        df_diagnoses,
        df_meds,
        df_genomics
    )

    # -----------------------------------
    # 7. Analytics
    # -----------------------------------
    print("\n[5] Running Analytics Modules...")

    run_patient_analytics(master_df)
    run_lab_analytics(df_labs)
    run_diagnosis_analytics(df_diagnoses)
    run_genomics_analytics(df_genomics)

    run_high_risk_analytics(
        master_df,
        df_labs,
        df_genomics
    )

    run_anomaly_detection(
        df_patients,
        df_labs,
        df_diagnoses,
        df_meds,
        df_genomics
    )

    # -----------------------------------
    # 8. Visualizations
    # -----------------------------------
    print("\n[6] Rendering Visualizations...")
    generate_visualizations()

    # -----------------------------------
    # 9. Manifest generation
    # -----------------------------------
    print("\n[7] Generating Manifests...")
    generate_all_manifests()

    print("=" * 60)
    print("Pipeline Execution Complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
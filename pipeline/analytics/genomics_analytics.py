import os
import pandas as pd


CONSUMPTION_DIR = "datalake/consumption"


def run_genomics_analytics(df_genomics):
    print("Running genomics analytics...")

    if df_genomics.empty:
        print("Genomics dataset empty.")
        return

    if "gene" not in df_genomics.columns:
        print("Gene column missing.")
        return

    stats = (
        df_genomics.groupby("gene")
        .agg(
            variant_count=("gene", "count")
        )
        .reset_index()
        .sort_values(
            by="variant_count",
            ascending=False
        )
        .head(5)
    )

    output_path = os.path.join(
        CONSUMPTION_DIR,
        "variant_hotspots.parquet"
    )

    stats.to_parquet(output_path, index=False)
    print("Saved variant_hotspots.parquet")


def run_high_risk_analytics(master_df, df_labs, df_genomics):
    print("Running high risk analytics...")

    if df_labs.empty or df_genomics.empty:
        print("Missing labs/genomics data.")
        return

    # -----------------------------------
    # Detect correct lab result column
    # -----------------------------------
    possible_result_cols = [
        "result_value",
        "value",
        "lab_value",
        "result",
        "test_result"
    ]

    result_col = None

    for col in possible_result_cols:
        if col in df_labs.columns:
            result_col = col
            break

    if result_col is None:
        print("No valid lab result column found.")
        return

    # -----------------------------------
    # Detect correct patient column
    # -----------------------------------
    possible_patient_cols = [
        "patient_id",
        "patientid",
        "id"
    ]

    patient_col = None

    for col in possible_patient_cols:
        if col in df_labs.columns:
            patient_col = col
            break

    if patient_col is None:
        print("No patient column found in labs.")
        return

    df_labs[result_col] = pd.to_numeric(
        df_labs[result_col],
        errors="coerce"
    )

    high_hba1c = df_labs[
        df_labs[result_col] > 7
    ][patient_col].astype(str)

    genomics_patients = set(
        df_genomics["patient_id"].astype(str)
    )

    high_risk = [
        pid for pid in high_hba1c
        if pid in genomics_patients
    ]

    high_risk_df = pd.DataFrame({
        "patient_id": high_risk
    })

    output_path = os.path.join(
        CONSUMPTION_DIR,
        "high_risk_patients.parquet"
    )

    high_risk_df.to_parquet(
        output_path,
        index=False
    )

    print("Saved high_risk_patients.parquet")
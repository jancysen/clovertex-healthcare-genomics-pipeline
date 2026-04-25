import os
import pandas as pd

CONSUMPTION_DIR = "datalake/consumption"


def run_anomaly_detection(
    df_patients,
    df_labs,
    df_diagnoses,
    df_meds,
    df_genomics
):
    print("Running anomaly detection engines...")

    anomalies = []

    # -------------------------------
    # Age anomalies
    # -------------------------------
    if not df_patients.empty and "age" in df_patients.columns:
        bad_age = df_patients[
            (df_patients["age"] < 0) |
            (df_patients["age"] > 120)
        ]

        for _, row in bad_age.iterrows():
            anomalies.append({
                "patient_id": row.get("patient_id", "unknown"),
                "anomaly_type": "Impossible Age",
                "reason": "Age less than 0 or greater than 120",
                "severity": "High"
            })

    # -------------------------------
    # Lab anomalies
    # -------------------------------
    if not df_labs.empty:
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

        if result_col:
            df_labs[result_col] = pd.to_numeric(
                df_labs[result_col],
                errors="coerce"
            )

            extreme_labs = df_labs[
                df_labs[result_col] > 1000
            ]

            for _, row in extreme_labs.iterrows():
                anomalies.append({
                    "patient_id": row.get("patient_id", "unknown"),
                    "anomaly_type": "Extreme Lab Value",
                    "reason": "Lab value exceeds safe threshold",
                    "severity": "Medium"
                })
        else:
            print("No valid lab result column found. Skipping lab anomaly checks.")

    # -------------------------------
    # Genomics anomalies
    # -------------------------------
    if not df_genomics.empty:
        possible_patient_cols = [
            "patient_id",
            "patientid",
            "id",
            "patient_Id"
        ]

        genomics_pid_col = None

        for col in possible_patient_cols:
            if col in df_genomics.columns:
                genomics_pid_col = col
                break

        if genomics_pid_col:
            missing_pid = df_genomics[
                df_genomics[genomics_pid_col].isnull()
            ]

            for _, row in missing_pid.iterrows():
                anomalies.append({
                    "patient_id": "unknown",
                    "anomaly_type": "Missing Genomics Patient ID",
                    "reason": "Missing patient linkage",
                    "severity": "High"
                })
        else:
            print("No genomics patient column found. Skipping genomics anomaly checks.")

    anomaly_df = pd.DataFrame(anomalies)

    output_path = os.path.join(
        CONSUMPTION_DIR,
        "anomaly_flags.parquet"
    )

    anomaly_df.to_parquet(
        output_path,
        index=False
    )

    print("Saved anomaly_flags.parquet")
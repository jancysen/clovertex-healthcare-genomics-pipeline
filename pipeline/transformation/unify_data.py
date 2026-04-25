import pandas as pd


def unify_data(df_patients, df_labs, df_diagnoses, df_meds, df_genomics):
    """
    Unify all datasets into a master patient table
    """

    print("Unifying datasets...")

    if df_patients.empty:
        print("Patients dataset empty.")
        return pd.DataFrame()

    master_df = df_patients.copy()

    # -----------------------------------
    # Fix lab patient column dynamically
    # -----------------------------------
    if not df_labs.empty:
        possible_lab_patient_cols = [
            "patient_id",
            "patientid",
            "patient_Id",
            "id"
        ]

        lab_patient_col = None

        for col in possible_lab_patient_cols:
            if col in df_labs.columns:
                lab_patient_col = col
                break

        if lab_patient_col:
            print(f"Using lab patient column: {lab_patient_col}")

            df_labs[lab_patient_col] = df_labs[lab_patient_col].astype(str)

            lab_summary = df_labs.groupby(lab_patient_col).size().reset_index(
                name="lab_record_count"
            )

            lab_summary.rename(
                columns={lab_patient_col: "patient_id"},
                inplace=True
            )

            master_df = master_df.merge(
                lab_summary,
                on="patient_id",
                how="left"
            )

    # -----------------------------------
    # Diagnoses merge
    # -----------------------------------
    if not df_diagnoses.empty and "patient_id" in df_diagnoses.columns:
        diagnosis_summary = df_diagnoses.groupby("patient_id").size().reset_index(
            name="diagnosis_count"
        )

        master_df = master_df.merge(
            diagnosis_summary,
            on="patient_id",
            how="left"
        )

    # -----------------------------------
    # Medications merge
    # -----------------------------------
    if not df_meds.empty and "patient_id" in df_meds.columns:
        med_summary = df_meds.groupby("patient_id").size().reset_index(
            name="medication_count"
        )

        master_df = master_df.merge(
            med_summary,
            on="patient_id",
            how="left"
        )

    # -----------------------------------
    # Genomics merge
    # -----------------------------------
    if not df_genomics.empty and "patient_id" in df_genomics.columns:
        genomics_summary = df_genomics.groupby("patient_id").size().reset_index(
            name="genomics_variant_count"
        )

        master_df = master_df.merge(
            genomics_summary,
            on="patient_id",
            how="left"
        )

    # Fill nulls
    master_df = master_df.fillna(0)

    print("Master dataset unified successfully.")

    return master_df
import pandas as pd
from pipeline.utils.logger import get_logger


def clean_patients_data(df, dataset_name="site_alpha_patients"):
    """
    Cleans patient data by handling:
    - duplicates
    - nulls
    - invalid dates
    - schema inconsistencies
    - datatype issues
    """

    if df.empty:
        return df

    rows_in = len(df)

    issues_found = {
        "duplicates_removed": 0,
        "nulls_handled": 0,
        "invalid_dates_fixed": 0,
        "schema_fixed": 0,
        "orphans_removed": 0
    }

    # ---------------------------------------------------
    # Standardize column names
    # ---------------------------------------------------
    original_cols = list(df.columns)

    df.columns = [
        c.strip().lower().replace(" ", "_")
        for c in df.columns
    ]

    if list(df.columns) != original_cols:
        issues_found["schema_fixed"] += 1

    # ---------------------------------------------------
    # Fix unhashable dict/list values before duplicate check
    # site_beta_patients.json may contain nested objects
    # ---------------------------------------------------
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: str(x) if isinstance(x, (dict, list)) else x
        )

    # ---------------------------------------------------
    # Handle duplicates
    # ---------------------------------------------------
    dup_count = df.duplicated().sum()

    if dup_count > 0:
        df = df.drop_duplicates()
        issues_found["duplicates_removed"] = int(dup_count)

    # ---------------------------------------------------
    # Ensure patient_id is string
    # ---------------------------------------------------
    if "patient_id" in df.columns:
        df["patient_id"] = df["patient_id"].astype(str)

    # ---------------------------------------------------
    # Remove rows with missing patient_id
    # ---------------------------------------------------
    if "patient_id" in df.columns:
        null_id_count = df["patient_id"].isnull().sum()

        if null_id_count > 0:
            df = df.dropna(subset=["patient_id"])
            issues_found["nulls_handled"] += int(null_id_count)

    # ---------------------------------------------------
    # Handle missing demographic values
    # ---------------------------------------------------
    for col in ["age", "gender"]:
        if col in df.columns:
            null_count = df[col].isnull().sum()

            if null_count > 0:
                if col == "age":
                    median_age = df[col].median()

                    if pd.isna(median_age):
                        median_age = 0

                    df[col] = df[col].fillna(median_age)

                elif col == "gender":
                    df[col] = df[col].fillna("Unknown")

                issues_found["nulls_handled"] += int(null_count)

    # ---------------------------------------------------
    # Handle invalid date columns
    # ---------------------------------------------------
    date_cols = [
        c for c in df.columns
        if "date" in c or "timestamp" in c
    ]

    for col in date_cols:
        invalid_dates = pd.to_datetime(
            df[col],
            errors="coerce"
        ).isnull().sum()

        if invalid_dates > 0:
            df[col] = pd.to_datetime(
                df[col],
                errors="coerce"
            )

            issues_found["invalid_dates_fixed"] += int(invalid_dates)

    rows_out = len(df)

    # ---------------------------------------------------
    # Structured JSON logging
    # ---------------------------------------------------
    get_logger().log_process(
        dataset_name,
        rows_in,
        rows_out,
        issues_found
    )

    return df
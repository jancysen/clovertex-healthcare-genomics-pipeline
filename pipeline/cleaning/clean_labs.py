import pandas as pd
from pipeline.utils.logger import get_logger

def clean_labs_data(df, valid_patient_ids, dataset_name="site_gamma_lab_results"):
    """
    Cleans lab results, removes orphans, drops invalid types/nulls.
    valid_patient_ids is a set or series of unified patient IDs.
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

    # Duplicates
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        df = df.drop_duplicates()
        issues_found["duplicates_removed"] = int(dup_count)
        
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Handle patient_id string
    if "patient_id" in df.columns:
        df["patient_id"] = df["patient_id"].astype(str)
        # Handle Orphans
        orphan_mask = ~df["patient_id"].isin(valid_patient_ids)
        orphan_count = orphan_mask.sum()
        if orphan_count > 0:
            df = df[~orphan_mask]
            issues_found["orphans_removed"] = int(orphan_count)

    # Missing critical lab values or test types
    critical_cols = ["test_type", "result_value", "test_date"]
    for col in critical_cols:
        if col in df.columns:
            nulls = df[col].isnull().sum()
            if nulls > 0:
                df = df.dropna(subset=[col])
                issues_found["nulls_handled"] += int(nulls)

    # Date correction if needed
    if "test_date" in df.columns:
        df["test_date"] = pd.to_datetime(df["test_date"], errors='coerce')
        invalid = df["test_date"].isnull().sum()
        if invalid > 0:
            df = df.dropna(subset=["test_date"])
            issues_found["invalid_dates_fixed"] += int(invalid)

    rows_out = len(df)
    get_logger().log_process(dataset_name, rows_in, rows_out, issues_found)
    return df

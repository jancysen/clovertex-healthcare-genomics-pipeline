import pandas as pd
from pipeline.utils.logger import get_logger
from pipeline.utils.config import GENOMICS_MIN_READ_DEPTH, GENOMICS_MIN_QUALITY_SCORE, GENOMICS_VALID_SIGNIFICANCE

def clean_genomics_data(df, valid_patient_ids, dataset_name="genomics_variants"):
    """
    Cleans genomics data utilizing configurable thresholds to filter
    unreliable variants, and remove orphan entries.
    """
    if df.empty:
        return df

    rows_in = len(df)
    issues_found = {
        "duplicates_removed": 0,
        "nulls_handled": 0,
        "unreliable_variants_removed": 0,
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

    # Filtering unreliable genomics variants
    # Assume cols: read_depth, quality_score, clinical_significance
    prev_len = len(df)
    
    if "read_depth" in df.columns:
        df = df[df["read_depth"] >= GENOMICS_MIN_READ_DEPTH]
    if "quality_score" in df.columns:
        df = df[df["quality_score"] >= GENOMICS_MIN_QUALITY_SCORE]
    if "clinical_significance" in df.columns:
        # Standardize strings
        df["clinical_significance"] = df["clinical_significance"].astype(str).str.strip().str.title()
        valid_lower = [v.lower() for v in GENOMICS_VALID_SIGNIFICANCE]
        df = df[df["clinical_significance"].str.lower().isin(valid_lower)]
        
    unreliable_count = prev_len - len(df)
    issues_found["unreliable_variants_removed"] += unreliable_count

    # Fill basic nulls in essential fields without dropping if not necessary
    if "gene" in df.columns:
        null_genes = df["gene"].isnull().sum()
        if null_genes > 0:
            df = df.dropna(subset=["gene"])
            issues_found["nulls_handled"] += int(null_genes)

    rows_out = len(df)
    get_logger().log_process(dataset_name, rows_in, rows_out, issues_found)
    return df

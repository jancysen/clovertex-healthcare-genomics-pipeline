import pandas as pd
import os
from pipeline.utils.config import CONSUMPTION_DIR, REF_ICD10

def run_diagnosis_analytics(df_diagnoses):
    """
    Generates diagnosis_frequency.parquet:
    - top ICD10 chapters
    - maps ICD codes using reference file
    """
    if df_diagnoses.empty or "icd10_code" not in df_diagnoses.columns:
        return
        
    print("Running diagnosis analytics...")
    
    df_diag = df_diagnoses.copy()
    
    # Map using Reference file
    if os.path.exists(REF_ICD10):
        ref_df = pd.read_csv(REF_ICD10)
        # assuming ref_df has 'icd10_code' and 'chapter_name'
        if "icd10_code" in ref_df.columns and "chapter_name" in ref_df.columns:
            df_diag = df_diag.merge(ref_df[["icd10_code", "chapter_name"]], on="icd10_code", how="left")
            
    # Calculate Frequency
    # If chapter_name missing, fallback to the code itself or prefix
    if "chapter_name" not in df_diag.columns:
        df_diag["chapter_name"] = df_diag["icd10_code"].str[0] # First letter of ICD is chapter
        
    freq_df = df_diag["chapter_name"].value_counts().reset_index()
    freq_df.columns = ["icd10_chapter", "frequency"]
    
    out_path = os.path.join(CONSUMPTION_DIR, "diagnosis_frequency.parquet")
    freq_df.to_parquet(out_path, engine="pyarrow", index=False)

import pandas as pd
import os
from pipeline.utils.config import CONSUMPTION_DIR

def run_patient_analytics(df_master):
    """
    Generates patient_summary.parquet:
    - age distribution
    - gender split
    - site distribution
    """
    if df_master.empty:
        return
        
    print("Running patient analytics...")
    
    # Let's pivot this into a simple aggregate or individual distributions.
    # The prompt says: "Generate EXACT parquet outputs: patient_summary.parquet - age dist, gender split, site dist"
    # To store this in a single parquet, we can create a 1-row summary or a melted metric table.
    
    # Melted approach:
    summary_data = []

    if "age" in df_master.columns:
        age_bins = pd.cut(df_master["age"], bins=[0, 18, 30, 50, 70, 120], labels=["0-18", "19-30", "31-50", "51-70", "71+"])
        age_dist = age_bins.value_counts().to_dict()
        for k, v in age_dist.items():
            summary_data.append({"category": "age_distribution", "metric": str(k), "value": v})

    if "gender" in df_master.columns:
        gender_dist = df_master["gender"].value_counts().to_dict()
        for k, v in gender_dist.items():
             summary_data.append({"category": "gender_split", "metric": str(k), "value": v})

    if "site_id" in df_master.columns or "site" in df_master.columns:
        site_col = "site_id" if "site_id" in df_master.columns else "site"
        site_dist = df_master[site_col].value_counts().to_dict()
        for k, v in site_dist.items():
             summary_data.append({"category": "site_distribution", "metric": str(k), "value": v})
             
    summary_df = pd.DataFrame(summary_data)
    out_path = os.path.join(CONSUMPTION_DIR, "patient_summary.parquet")
    summary_df.to_parquet(out_path, engine="pyarrow", index=False)

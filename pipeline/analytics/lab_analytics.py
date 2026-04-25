import pandas as pd
import os
import json
from pipeline.utils.config import CONSUMPTION_DIR, REF_LAB_RANGES

def run_lab_analytics(df_labs):
    """
    Generates lab_statistics.parquet:
    - mean, median, std dev, out-of-range flag for each test
    - improving/worsening trends for HbA1c/Creatinine
    """
    if df_labs.empty or "test_type" not in df_labs.columns or "result_value" not in df_labs.columns:
        return
        
    print("Running lab analytics...")

    # Load Reference Ranges if available
    lab_ranges = {}
    if os.path.exists(REF_LAB_RANGES):
        with open(REF_LAB_RANGES, "r") as f:
            lab_ranges = json.load(f)

    # Calculate basic stats
    df_labs["result_value"] = pd.to_numeric(df_labs["result_value"], errors="coerce")
    valid_labs = df_labs.dropna(subset=["result_value"])
    
    stats_df = valid_labs.groupby("test_type").agg(
        mean_val=("result_value", "mean"),
        median_val=("result_value", "median"),
        std_dev=("result_value", "std")
    ).reset_index()

    # Out of range flag
    def check_out_of_range(row):
        test = row["test_type"]
        val = row["mean_val"] # We flag if the mean is out of expected healthy range, or we can check percentages.
        if test in lab_ranges:
            low = lab_ranges[test].get("low", 0)
            high = lab_ranges[test].get("high", 100)
            return True if (val < low or val > high) else False
        return False
        
    stats_df["mean_out_of_range"] = stats_df.apply(check_out_of_range, axis=1)

    # Trends for HbA1c + Creatinine
    # Trend = (Last Val - First Val). If positive (increasing), could be worsening.
    # Group by patient_id and test_type, sort by date
    if "test_date" in valid_labs.columns and "patient_id" in valid_labs.columns:
        trend_tests = valid_labs[valid_labs["test_type"].str.contains("HbA1c|Creatinine", case=False, na=False)]
        if not trend_tests.empty:
             sorted_labs = trend_tests.sort_values(by=["patient_id", "test_type", "test_date"])
             trends = []
             for (pid, test), grp in sorted_labs.groupby(["patient_id", "test_type"]):
                 if len(grp) > 1:
                     first_val = grp["result_value"].iloc[0]
                     last_val = grp["result_value"].iloc[-1]
                     diff = last_val - first_val
                     # Simplify: increasing HbA1c/Creatinine is generally worsening
                     status = "worsening" if diff > 0 else "improving" if diff < 0 else "stable"
                 else:
                     status = "insufficient_data"
                 trends.append({"patient_id": pid, "test_type": test, "trend": status})
             
             trend_df = pd.DataFrame(trends)
             # Summarize trend count
             trend_summary = trend_df.groupby(["test_type", "trend"]).size().reset_index(name="count")
             # We merge this string info into the stats_df
             trend_pivot = trend_summary.pivot(index="test_type", columns="trend", values="count").reset_index()
             stats_df = pd.merge(stats_df, trend_pivot, on="test_type", how="left")

    out_path = os.path.join(CONSUMPTION_DIR, "lab_statistics.parquet")
    stats_df.to_parquet(out_path, engine="pyarrow", index=False)

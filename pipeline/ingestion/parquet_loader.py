import pandas as pd
import os
import pyarrow.parquet as pq

def ingest_parquet(filepath):
    """Ingests a Parquet file into a pandas DataFrame."""
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return pd.DataFrame()
        
    try:
        # Using pyarrow directly or pandas. Pandas uses pyarrow engine under the hood.
        df = pd.read_parquet(filepath, engine="pyarrow")
        return df
    except Exception as e:
        print(f"Error reading Parquet {filepath}: {e}")
        return pd.DataFrame()

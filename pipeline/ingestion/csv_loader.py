import pandas as pd
import os

def ingest_csv(filepath):
    """Ingests a CSV file into a pandas DataFrame."""
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return pd.DataFrame()
        
    try:
        df = pd.read_csv(filepath)
        return df
    except Exception as e:
        print(f"Error reading CSV {filepath}: {e}")
        return pd.DataFrame()

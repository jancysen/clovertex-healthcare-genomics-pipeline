import pandas as pd
import os

def ingest_json(filepath, lines=False):
    """Ingests a JSON file into a pandas DataFrame."""
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return pd.DataFrame()
        
    try:
        df = pd.read_json(filepath, lines=lines)
        return df
    except ValueError:
        # Fallback to the other mode if the explicit parameter fails
        try:
            df = pd.read_json(filepath, lines=not lines)
            return df
        except Exception as e:
            print(f"Error reading JSON {filepath}: {e}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error reading JSON {filepath}: {e}")
        return pd.DataFrame()

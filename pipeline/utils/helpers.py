import os
import shutil
from pipeline.utils.config import RAW_DIR, REFINED_DIR, CONSUMPTION_DIR, PLOTS_DIR, DATA_DIR

def setup_datalake():
    """Creates the data lake directory structure if it doesn't exist."""
    dirs_to_create = [RAW_DIR, REFINED_DIR, CONSUMPTION_DIR, PLOTS_DIR]
    
    for d in dirs_to_create:
        os.makedirs(d, exist_ok=True)
        print(f"Ensured directory exists: {d}")

def copy_to_raw():
    """Copies original files untouched to the raw/ layer stringently."""
    setup_datalake()
    
    if not os.path.exists(DATA_DIR):
        print(f"Warning: Data directory {DATA_DIR} does not exist.")
        return

    # Files to copy include anything in data/ but avoid directories for simplicity
    for item in os.listdir(DATA_DIR):
        item_path = os.path.join(DATA_DIR, item)
        if os.path.isfile(item_path):
            shutil.copy2(item_path, RAW_DIR)
            
    print("Copied all source files to datalake/raw/")

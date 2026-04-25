import os
import json
import hashlib
import pandas as pd
from datetime import datetime, timezone
from pipeline.utils.config import BASE_DIR, RAW_DIR, REFINED_DIR, CONSUMPTION_DIR

def sha256_checksum(filepath):
    """Generates SHA256 checksum for a file."""
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def get_file_metadata(filepath):
    """Extracts schema and row count using pandas/pyarrow based on extension."""
    ext = os.path.splitext(filepath)[1].lower()
    row_count = 0
    schema = {}
    
    try:
        if ext == '.csv':
            df = pd.read_csv(filepath)
            row_count = len(df)
            schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
        elif ext == '.json':
            # Handle possible lines=True or standard JSON format
            try:
                df = pd.read_json(filepath)
            except ValueError:
                df = pd.read_json(filepath, lines=True)
            row_count = len(df)
            schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
        elif ext == '.parquet':
            df = pd.read_parquet(filepath)
            row_count = len(df)
            schema = {col: str(dtype) for col, dtype in df.dtypes.items()}
    except Exception as e:
        print(f"Could not read metadata for {filepath}: {e}")

    return row_count, schema

def generate_manifest(layer_dir):
    """
    Generates manifest.json for a specific datalake layer (raw, refined, consumption).
    """
    manifest_entries = []
    
    # Walk through the directory and compute stats for all files
    for root, _, files in os.walk(layer_dir):
        for file in files:
            if file == "manifest.json" or file.endswith(".md"):
                continue # Skip manifest itself and readmes
                
            file_path = os.path.join(root, file)
            # path relative to project
            rel_path = os.path.relpath(file_path, BASE_DIR)
            
            row_count, schema = get_file_metadata(file_path)
            checksum = sha256_checksum(file_path)
            
            entry = {
                "filename": file,
                "file_path": rel_path,
                "row_count": row_count,
                "schema": schema,
                "processing_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "sha256_checksum": checksum
            }
            manifest_entries.append(entry)
            
    # Save manifest
    manifest_path = os.path.join(layer_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest_entries, f, indent=4)
        
    print(f"Generated manifest for {layer_dir} with {len(manifest_entries)} files.")

def generate_all_manifests():
    for layer in [RAW_DIR, REFINED_DIR, CONSUMPTION_DIR]:
        # Exclude empty layers if any
        if os.path.exists(layer):
             generate_manifest(layer)

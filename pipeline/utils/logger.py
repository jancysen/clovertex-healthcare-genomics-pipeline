import json
from datetime import datetime, timezone
import os
from pipeline.utils.config import BASE_DIR

class ProcessLogger:
    def __init__(self):
        self.quality_report = []

    def log_process(self, dataset_name, rows_in, rows_out, issues_dict):
        """
        Logs processing step exactly as requested in specifications.
        """
        log_entry = {
            "dataset": dataset_name,
            "rows_in": rows_in,
            "rows_out": rows_out,
            "issues_found": issues_dict,
            "processing_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        # Print structured JSON log exactly as requested
        print(json.dumps(log_entry, indent=2))
        
        # Keep track for the final summary data_quality_report.json
        self.quality_report.append(log_entry)

    def write_quality_report(self):
        report_path = BASE_DIR / "data_quality_report.json"
        
        # Calculate summary across all datasets to match requirements
        summary = {
            "nulls_handled": sum([entry["issues_found"].get("nulls_handled", 0) for entry in self.quality_report]),
            "duplicates_removed": sum([entry["issues_found"].get("duplicates_removed", 0) for entry in self.quality_report]),
            "orphan_records": sum([entry["issues_found"].get("orphans_removed", 0) for entry in self.quality_report]),
            "schema_mismatches": sum([entry["issues_found"].get("schema_fixed", 0) for entry in self.quality_report]),
            "invalid_dates_fixed": sum([entry["issues_found"].get("invalid_dates_fixed", 0) for entry in self.quality_report]),
            "total_rows_before": sum([entry["rows_in"] for entry in self.quality_report]),
            "total_rows_after": sum([entry["rows_out"] for entry in self.quality_report]),
            "dataset_logs": self.quality_report
        }
        
        with open(report_path, "w") as f:
            json.dump(summary, f, indent=4)
        print(f"Data quality report saved to {report_path}")

process_logger = ProcessLogger()

def get_logger():
    return process_logger

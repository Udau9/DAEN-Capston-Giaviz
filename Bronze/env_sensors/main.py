import os
from datetime import datetime
import pandas as pd
from ingestion import ingest_data
from validation import validate_data, VALIDATION_RULES

# --- Configuration ---
# Change this path to the directory where you want to save the output files
BASE_OUTPUT_PATH = os.path.join(os.path.expanduser('~'), 'Downloads', 'data_pipeline_output')

def run_pipeline():
    """
    Orchestrates the data pipeline: ingestion, validation, and saving.
    """
    print("Starting data ingestion...")
    raw_xml, long_df = ingest_data()

    if long_df is None:
        print("Pipeline aborted due to ingestion error.")
        return

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Define full paths using the base output path
    bronze_path = os.path.join(BASE_OUTPUT_PATH, "bronze")
    silver_path = os.path.join(BASE_OUTPUT_PATH, "silver")
    quarantine_path = os.path.join(BASE_OUTPUT_PATH, "quarantine")

    # 1. Save the raw, unvalidated data to the Bronze layer
    os.makedirs(bronze_path, exist_ok=True)
    long_df.to_csv(os.path.join(bronze_path, f"raw_sensors_{timestamp}.csv"), index=False)
    print("✅ Raw, ingested data saved to Bronze folder.")

    # 2. Perform cleaning and standardization
    print("\nStarting data cleaning and standardization...")
    print("✅ Data has been standardized into a long-format DataFrame.")

    # 3. Perform validation on the cleaned data
    print("\nStarting data validation...")
    clean_df, quarantine_df = validate_data(long_df, VALIDATION_RULES)
    
    # 4. Save the clean and invalid data to their respective layers
    os.makedirs(silver_path, exist_ok=True)
    os.makedirs(quarantine_path, exist_ok=True)
    
    if not clean_df.empty:
        clean_df.to_csv(os.path.join(silver_path, f"clean_sensors_{timestamp}.csv"), index=False)
        print("✅ Valid data saved to Silver CSV tables.")
    else:
        print("No valid data to save to Silver folder.")

    if not quarantine_df.empty:
        quarantine_df.to_csv(os.path.join(quarantine_path, f"invalid_sensors_{timestamp}.csv"), index=False)
        print("✅ Invalid data saved to Quarantine folder.")
    else:
        print("No invalid data to quarantine.")

    print("\n--- Pipeline Summary ---")
    print(f"Total Records Ingested: {len(long_df)}")
    print(f"Valid Records Saved:    {len(clean_df)}")
    print(f"Invalid Records Quarantined: {len(quarantine_df)}")

if __name__ == "__main__":
    run_pipeline()

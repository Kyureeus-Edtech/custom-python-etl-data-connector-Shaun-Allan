# from pipeline import extract_nvd_data, transform_nvd_data, load_data
from pipeline import extract_nvd_data, transform_nvd_data, load_data
from config import (
    NVD_API_BASE_URL,
    NVD_API_KEY,
    MONGO_URI,
    DB_NAME,
    COLLECTION_NAME
)

def run_pipeline():
    """
    Executes the full ETL pipeline for NVD CVE data.
    """
    print("--- Starting NVD CVE Vulnerability ETL Pipeline ---")
    
    # Step 1: EXTRACT
    raw_data = extract_nvd_data(NVD_API_BASE_URL, NVD_API_KEY)
    if not raw_data:
        print("Stopping: No new data extracted.")
        return

    # Step 2: TRANSFORM
    transformed_data = transform_nvd_data(raw_data)
    
    # Step 3: LOAD
    if transformed_data:
        load_data(transformed_data, MONGO_URI, DB_NAME, COLLECTION_NAME)
            
    print("\n--- ETL Pipeline Finished ---")

if __name__ == "__main__":
    run_pipeline()
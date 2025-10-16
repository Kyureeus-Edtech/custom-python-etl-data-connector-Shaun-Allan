# main.py
from etl.etl_connector import run_full_etl
from utils.logger import setup_logger

if __name__ == "__main__":
    # Setup logging first
    setup_logger()
    
    # Run the ETL pipeline
    run_full_etl() 
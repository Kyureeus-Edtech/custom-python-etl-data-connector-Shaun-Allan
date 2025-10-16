# utils/logger.py
import logging
import sys

def setup_logger():
    """Configures the root logger for the application."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout) # You can also add a FileHandler here
        ]
    )
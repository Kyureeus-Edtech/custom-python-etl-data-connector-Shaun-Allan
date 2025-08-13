import os
from dotenv import load_dotenv

# Load variables from the .env file into the environment
load_dotenv()

# --- NVD API Configuration ---
NVD_API_BASE_URL = os.getenv("NVD_API_BASE_URL")
NVD_API_KEY = os.getenv("NVD_API_KEY")


# --- MongoDB Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
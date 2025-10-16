# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# SonarQube Configuration
SONARQUBE_URL = os.getenv("SONARQUBE_URL", "http://localhost:9000")
SONARQUBE_API_TOKEN = os.getenv("SONARQUBE_API_TOKEN")

# MongoDB Configuration
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "sonarqube_data")
MONGO_PROJECTS_COLLECTION = os.getenv("MONGO_PROJECTS_COLLECTION", "projects")
MONGO_ISSUES_COLLECTION = os.getenv("MONGO_ISSUES_COLLECTION", "issues")
MONGO_MEASURES_COLLECTION = os.getenv("MONGO_MEASURES_COLLECTION", "measures")
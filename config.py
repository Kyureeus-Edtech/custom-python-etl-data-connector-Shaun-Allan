import os
from dotenv import load_dotenv

load_dotenv()

# --- Spotify API Configuration ---
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_BASE_API_URL = "https://api.spotify.com"


# --- MongoDB Configuration ---
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "spotify_db"
COLLECTION_NAME = "playlists"
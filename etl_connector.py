import argparse
from auth import get_spotify_token
from pipeline import (
    get_playlist_details, 
    get_playlist_track_ids,
    get_track_details,
    create_merged_document,
    load_data
)
from config import (
    SPOTIFY_CLIENT_ID,
    SPOTIFY_CLIENT_SECRET,
    SPOTIFY_AUTH_URL,
    SPOTIFY_BASE_API_URL,
    MONGO_URI,
    DB_NAME,
    COLLECTION_NAME
)

def run_pipeline(playlist_id):
    """
    Executes the ETL pipeline for a given playlist ID.
    """
    print("--- Starting Merged Spotify ETL Pipeline ---")
    
    token = get_spotify_token(SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, SPOTIFY_AUTH_URL)
    if not token:
        print("Stopping: Auth failed."); return

    # Step 1: EXTRACT ALL DATA
    playlist_details = get_playlist_details(token, playlist_id, SPOTIFY_BASE_API_URL)
    track_ids = get_playlist_track_ids(token, playlist_id, SPOTIFY_BASE_API_URL)
    track_details = get_track_details(token, track_ids, SPOTIFY_BASE_API_URL)

    if not all([playlist_details, track_details]):
        print("Stopping: Failed to extract all necessary data.")
        return

    # Step 2: TRANSFORM
    merged_data = create_merged_document(playlist_details, track_details)
    
    # Step 3: LOAD
    if merged_data:
        # The print statement is removed from the final version, but you can add it for debugging
        # print(merged_data)
        load_data(merged_data, MONGO_URI, DB_NAME, COLLECTION_NAME)
            
    print("\n--- ETL Pipeline Finished ---")





if __name__ == "__main__":
    # 1. Set up the argument parser
    parser = argparse.ArgumentParser(description="Run the Spotify ETL pipeline for a specific playlist.")
    
    # 2. Add the playlist_id argument
    parser.add_argument("playlist_id", help="The Spotify ID of the public playlist to process.")
    
    # 3. Parse the arguments from the command line
    args = parser.parse_args()
    
    # 4. Call the main function with the provided playlist ID
    run_pipeline(args.playlist_id)
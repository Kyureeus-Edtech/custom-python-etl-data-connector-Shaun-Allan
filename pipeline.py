import requests
from datetime import datetime, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure



#################################
#            EXTRACT            #
#################################

def get_playlist_details(token, playlist_id, base_url):
    """
    Extracts the full, raw JSON object for a playlist from the Spotify API.
    """
    print(f"Extracting details for playlist: {playlist_id}...")
    url = f"{base_url}/v1/playlists/{playlist_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to get playlist details:", response.json())
        return None
    return response.json()

def get_playlist_track_ids(token, playlist_id, base_url):
    """
    Extracts only the track IDs from a specific Spotify playlist.
    """
    print(f"Extracting track IDs from playlist: {playlist_id}...")
    url = f"{base_url}/v1/playlists/{playlist_id}/tracks"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to get playlist track IDs:", response.json())
        return None
    
    items = response.json().get('items', [])
    track_ids = [item['track']['id'] for item in items if item.get('track') and item['track'].get('id')]
    print(f"Found {len(track_ids)} track IDs.")
    return track_ids

def get_track_details(token, track_ids, base_url):
    """
    Extracts full details for a list of tracks using their IDs.
    """
    if not track_ids:
        return None
    print(f"Extracting full details for {len(track_ids)} tracks...")
    url = f"{base_url}/v1/tracks"
    headers = {"Authorization": f"Bearer {token}"}
    params = {"ids": ",".join(track_ids)}
    response = requests.get(url, headers=headers, params=params)
    if response.status_code != 200:
        print("Failed to get track details:", response.json())
        return None
    
    return response.json().get('tracks', [])




#################################
#           TRANSFORM           #
#################################

def transform_playlist_data(playlist_details):
    """
    Helper function to transform raw playlist data.
    """
    if not playlist_details: return None
    return {
        "playlist_id": playlist_details.get('id'),
        "name": playlist_details.get('name'),
        "description": playlist_details.get('description'),
        "owner": playlist_details.get('owner', {}).get('display_name'),
        "total_followers": playlist_details.get('followers', {}).get('total', 0),
        "ingestion_timestamp": datetime.now(timezone.utc)
    }

def transform_track_data(track_details):
    """
    Helper function to transform a list of raw track data.
    """
    if not track_details: return []
    
    transformed_records = []
    current_year = datetime.now(timezone.utc).year

    for track in track_details:
        if not track: continue
        # 1. Calculate Album Age
        try:
            release_year = int(track['album']['release_date'].split('-')[0])
            album_age = current_year - release_year
        except (ValueError, IndexError, TypeError):
            album_age = None
            
        # 2. Check Market Availability
        is_available_in_india = "IN" in track.get('available_markets', [])
        
        # 3. Categorize Popularity
        popularity = track.get('popularity', 0)
        if popularity > 75:
            popularity_tier = "Mainstream Hit"
        elif popularity > 50:
            popularity_tier = "Popular"
        else:
            popularity_tier = "Niche"

        clean_record = {
            "track_id": track.get('id'),
            "track_name": track.get('name'),
            "artists": [artist['name'] for artist in track.get('artists', [])],
            "album_name": track.get('album', {}).get('name'),
            "album_age_years": album_age,
            "is_available_in_india": is_available_in_india,
            "popularity_tier": popularity_tier
        }
        transformed_records.append(clean_record)
        
    return transformed_records

def create_merged_document(playlist_details, track_details):
    """
    Main transformation function that merges playlist and track data.
    """
    print("Creating merged document...")
    merged_document = transform_playlist_data(playlist_details)
    if not merged_document:
        return None
    
    transformed_tracks = transform_track_data(track_details)
    
    merged_document['tracks'] = transformed_tracks
    
    print("Merged document created successfully.")
    return merged_document



#################################
#             LOAD              #
#################################

def load_data(data, mongo_uri, db_name, collection_name):
    """
    Loads a single merged document into MongoDB, replacing it if it exists.
    """
    if not data:
        print("No data to load.")
        return

    print(f"Connecting to MongoDB and loading data into '{collection_name}'...")
    client = None
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ismaster')
        db = client[db_name]
        collection = db[collection_name]
        
        # Replace the entire document for this playlist based on its ID
        # or insert it if it doesn't exist (upsert=True).
        collection.replace_one(
            {"playlist_id": data.get("playlist_id")}, 
            data, 
            upsert=True
        )
        print(f"Successfully upserted document for playlist: {data.get('playlist_id')}")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred during loading: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")
import requests
from tenacity import retry, stop_after_attempt, wait_exponential



@retry(
    stop=stop_after_attempt(3), # Retry up to 3 times
    wait=wait_exponential(multiplier=1, min=2, max=10) # Wait 2s, then 4s, etc.
)
def get_spotify_token(client_id, client_secret, auth_url):
    """
    Requests an access token from the Spotify Accounts service, with timeout and retries.
    """
    print("Requesting Spotify access token...")
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    }, timeout=10) # Wait a maximum of 10 seconds for a response
    
    auth_response.raise_for_status() # Raise an exception for bad responses to trigger retry
    
    auth_response_data = auth_response.json()
    if 'access_token' in auth_response_data:
        print("Access token received successfully.")
        return auth_response_data['access_token']
    else:
        print("Error getting access token:", auth_response_data)
        return None
import requests


def get_spotify_token(client_id, client_secret, auth_url):
    """
    Requests an access token from the Spotify Accounts service.
    """
    print("Requesting Spotify access token...")
    auth_response = requests.post(auth_url, {
        'grant_type': 'client_credentials',
        'client_id': client_id,
        'client_secret': client_secret,
    })
    auth_response_data = auth_response.json()
    if 'access_token' in auth_response_data:
        print("Access token received successfully.")
        return auth_response_data['access_token']
    else:
        print("Error getting access token:", auth_response_data)
        return None
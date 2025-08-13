import unittest
from unittest.mock import patch, MagicMock
import requests
from pymongo.errors import ConnectionFailure
from tenacity import RetryError # Import RetryError
from pipeline import get_playlist_details, get_track_details, load_data

SPIDERMAN_PLAYLIST_DATA = {
    'playlist_id': '70ROJroKWQhEKSDCzc3QG6', 'name': 'Spider man', 'description': '', 'owner': 'Shaun', 'total_followers': 0,
    'tracks': [
        {'track_id': '5rurggqwwudn9clMdcchxT', 'track_name': 'Calling (Spider-Man: Across the Spider-Verse) (Metro Boomin & Swae Lee, NAV, feat. A Boogie Wit da Hoodie)', 'artists': ['Metro Boomin', 'Swae Lee', 'NAV', 'A Boogie Wit da Hoodie'], 'album_name': 'METRO BOOMIN PRESENTS SPIDER-MAN: ACROSS THE SPIDER-VERSE (SOUNDTRACK FROM AND INSPIRED BY THE MOTION PICTURE)', 'album_age_years': 2, 'is_available_in_india': True, 'popularity_tier': 'Popular'},
        {'track_id': '6Ec5LeRzkisa5KJtwLfOoW', 'track_name': 'Am I Dreaming (Metro Boomin & A$AP Rocky, Roisee)', 'artists': ['Metro Boomin', 'A$AP Rocky', 'Roisee'], 'album_name': 'METRO BOOMIN PRESENTS SPIDER-MAN: ACROSS THE SPIDER-VERSE (SOUNDTRACK FROM AND INSPIRED BY THE MOTION PICTURE)', 'album_age_years': 2, 'is_available_in_india': True, 'popularity_tier': 'Mainstream Hit'},
        {'track_id': '0wt9RjddODlWDetpuaXfRK', 'track_name': 'Sunflower (Remix) [with Swae Lee, Nicky Jam, and Prince Royce]', 'artists': ['Post Malone', 'Swae Lee', 'Nicky Jam', 'Prince Royce', 'G.O.K.B.'], 'album_name': 'Spider-Man: Into the Spider-Verse (Deluxe Edition / Soundtrack From & Inspired By The Motion Picture)', 'album_age_years': 6, 'is_available_in_india': True, 'popularity_tier': 'Popular'},
        {'track_id': '39MK3d3fonIP8Mz9oHCTBB', 'track_name': 'Annihilate (Spider-Man: Across the Spider-Verse) (Metro Boomin & Swae Lee, Lil Wayne, Offset)', 'artists': ['Metro Boomin', 'Swae Lee', 'Lil Wayne', 'Offset'], 'album_name': 'METRO BOOMIN PRESENTS SPIDER-MAN: ACROSS THE SPIDER-VERSE (SOUNDTRACK FROM AND INSPIRED BY THE MOTION PICTURE)', 'album_age_years': 2, 'is_available_in_india': True, 'popularity_tier': 'Popular'},
        {'track_id': '0AAMnNeIc6CdnfNU85GwCH', 'track_name': 'Self Love (Spider-Man: Across the Spider-Verse) (Metro Boomin & Coi Leray)', 'artists': ['Metro Boomin', 'Coi Leray'], 'album_name': 'METRO BOOMIN PRESENTS SPIDER-MAN: ACROSS THE SPIDER-VERSE (SOUNDTRACK FROM AND INSPIRED BY THE MOTION PICTURE)', 'album_age_years': 2, 'is_available_in_india': True, 'popularity_tier': 'Popular'},
        {'track_id': '2TvzLO4ifNWEnAAShxA6YW', 'track_name': "What's Up Danger (with Black Caviar)", 'artists': ['Blackway', 'Black Caviar'], 'album_name': 'Spider-Man: Into the Spider-Verse (Deluxe Edition / Soundtrack From & Inspired By The Motion Picture)', 'album_age_years': 6, 'is_available_in_india': True, 'popularity_tier': 'Niche'},
        {'track_id': '3KkXRkHbMCARz0aVfEt68P', 'track_name': 'Sunflower - Spider-Man: Into the Spider-Verse', 'artists': ['Post Malone', 'Swae Lee'], 'album_name': 'Spider-Man: Into the Spider-Verse (Soundtrack From & Inspired by the Motion Picture)', 'album_age_years': 7, 'is_available_in_india': True, 'popularity_tier': 'Mainstream Hit'},
        {'track_id': '5zsHmE2gO3RefVsPyw2e3T', 'track_name': "What's Up Danger (with Black Caviar)", 'artists': ['Blackway', 'Black Caviar'], 'album_name': 'Spider-Man: Into the Spider-Verse (Soundtrack From & Inspired by the Motion Picture)', 'album_age_years': 7, 'is_available_in_india': True, 'popularity_tier': 'Popular'}
    ]
}


class TestPipelineRobustness(unittest.TestCase):

    @patch('pipeline.requests.get')
    def test_extract_handles_api_error(self, mock_get):
        """
        Test that the function raises a RetryError after failing on a 404 error.
        """
        print("\nTesting: API returns an invalid response (404)...")
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
        mock_get.return_value = mock_response
        
        # MODIFIED: We now assert that a RetryError is raised after all attempts fail
        with self.assertRaises(RetryError):
            get_playlist_details('fake_token', 'fake_id', 'http://fake.url')

    @patch('pipeline.requests.get')
    def test_extract_handles_rate_limit(self, mock_get):
        """
        Test that the function raises a RetryError after failing on a 429 error.
        """
        print("\nTesting: API returns a rate limit error (429)...")
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError
        mock_get.return_value = mock_response

        # MODIFIED: We now assert that a RetryError is raised
        with self.assertRaises(RetryError):
            get_playlist_details('fake_token', 'fake_id', 'http://fake.url')

    @patch('pipeline.requests.get')
    def test_extract_handles_network_error(self, mock_get):
        """
        Test that the function raises a RetryError after a network error.
        """
        print("\nTesting: Network connectivity issue to API...")
        mock_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")
        
        # MODIFIED: We now assert that a RetryError is raised
        with self.assertRaises(RetryError):
            get_playlist_details('fake_token', 'fake_id', 'http://fake.url')

    # (The tests that were already passing are unchanged)

    @patch('pipeline.requests.get')
    def test_extract_handles_empty_payload(self, mock_get):
        print("\nTesting: API returns a successful but empty payload...")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'tracks': []}
        mock_get.return_value = mock_response
        result = get_track_details('fake_token', ['some_id'], 'http://fake.url')
        self.assertEqual(result, [], "Function should return an empty list for an empty payload")

    @patch('pipeline.MongoClient')
    def test_load_handles_db_connection_error(self, mock_mongo_client):
        print("\nTesting: Database connectivity issue...")
        mock_mongo_client.side_effect = ConnectionFailure("Could not connect to MongoDB")
        load_data(SPIDERMAN_PLAYLIST_DATA, "fake_uri", "fake_db", "fake_collection")
        print("  -> Test passed: The function handled the exception gracefully.")

    @patch('pipeline.MongoClient')
    def test_load_data_calls_mongodb_correctly(self, mock_mongo_client):
        print("\nTesting: Correct data insertion into MongoDB...")
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_instance = mock_mongo_client.return_value
        mock_instance.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        load_data(SPIDERMAN_PLAYLIST_DATA, "fake_uri", "fake_db", "playlists_with_tracks")
        mock_collection.replace_one.assert_called_with(
            {"playlist_id": "70ROJroKWQhEKSDCzc3QG6"},
            SPIDERMAN_PLAYLIST_DATA,
            upsert=True
        )
        print("  -> Verified that MongoDB's replace_one() was called with the correct filter and data.")

if __name__ == '__main__':
    unittest.main()
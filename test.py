import unittest
from unittest.mock import patch, MagicMock, call
import requests
from pymongo.errors import ConnectionFailure
from tenacity import RetryError
from pipeline import extract_nvd_data, transform_nvd_data, load_data
from datetime import datetime

# Mock JSON response from the NVD API
MOCK_NVD_JSON_RESPONSE = {
    "resultsPerPage": 2,
    "startIndex": 0,
    "totalResults": 2,
    "format": "NVD_CVE",
    "version": "2.0",
    "timestamp": "2025-08-14T02:40:53.943",
    "vulnerabilities": [
        {
            "cve": {
                "id": "CVE-2025-0001",
                "published": "2025-07-20T18:15:09.917",
                "lastModified": "2025-08-10T08:15:11.783",
                "descriptions": [{"lang": "en", "value": "A critical vulnerability in a test web server."}],
                "metrics": {
                    "cvssMetricV31": [
                        {
                            "cvssData": {
                                "version": "3.1",
                                "vectorString": "CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H",
                                "baseScore": 9.8,
                                "baseSeverity": "CRITICAL"
                            }
                        }
                    ]
                }
            }
        },
        {
            "cve": {
                "id": "CVE-2025-0002",
                "published": "2025-07-21T10:00:00.000",
                "lastModified": "2025-08-11T12:00:00.000",
                "descriptions": [{"lang": "en", "value": "A medium severity issue in a database client."}],
                "metrics": {
                    "cvssMetricV31": [
                        {
                            "cvssData": {
                                "version": "3.1",
                                "vectorString": "CVSS:3.1/AV:L/AC:H/PR:L/UI:N/S:U/C:H/I:N/A:N",
                                "baseScore": 5.3,
                                "baseSeverity": "MEDIUM"
                            }
                        }
                    ]
                }
            }
        }
    ]
}

# Expected data after transformation
EXPECTED_TRANSFORMED_DATA = [
    {
        'cve_id': 'CVE-2025-0001',
        'published_date': '2025-07-20T18:15:09.917',
        'last_modified_date': '2025-08-10T08:15:11.783',
        'description': 'A critical vulnerability in a test web server.',
        'base_score': 9.8,
        'severity': 'CRITICAL',
        'vector_string': 'CVSS:3.1/AV:N/AC:L/PR:N/UI:N/S:U/C:H/I:H/A:H'
    },
    {
        'cve_id': 'CVE-2025-0002',
        'published_date': '2025-07-21T10:00:00.000',
        'last_modified_date': '2025-08-11T12:00:00.000',
        'description': 'A medium severity issue in a database client.',
        'base_score': 5.3,
        'severity': 'MEDIUM',
        'vector_string': 'CVSS:3.1/AV:L/AC:H/PR:L/UI:N/S:U/C:H/I:N/A:N'
    }
]


class TestNvdCvePipeline(unittest.TestCase):

    @patch('pipeline.requests.get')
    def test_extract_success(self, mock_get):
        """Test successful extraction of JSON data from NVD API."""
        print("\nTesting: Successful data extraction...")
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_NVD_JSON_RESPONSE
        mock_get.return_value = mock_response

        # We only need to provide the URL, the function calculates the params
        result = extract_nvd_data("http://fake.nvd.url")
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['cve']['id'], 'CVE-2025-0001')
        print("  -> Passed.")

    @patch('pipeline.requests.get')
    def test_extract_handles_network_error(self, mock_get):
        """Test that the extract function retries and raises RetryError on network failure."""
        print("\nTesting: Network connectivity issue to API...")
        mock_get.side_effect = requests.exceptions.ConnectionError("Failed to connect")
        
        with self.assertRaises(RetryError):
            extract_nvd_data('http://fake.nvd.url')
        print("  -> Passed: RetryError was correctly raised.")

    def test_transform_data(self):
        """Test the data transformation logic for nested NVD JSON."""
        print("\nTesting: Data transformation...")
        raw_data = MOCK_NVD_JSON_RESPONSE['vulnerabilities']
        result = transform_nvd_data(raw_data)
        
        # We manually remove the 'ingestion_timestamp' for a predictable comparison
        for record in result:
            del record['ingestion_timestamp']
            
        self.assertEqual(len(result), 2)
        self.assertEqual(result, EXPECTED_TRANSFORMED_DATA)
        print("  -> Passed.")

    @patch('pipeline.MongoClient')
    def test_load_handles_db_connection_error(self, mock_mongo_client):
        """Test that the load function handles DB connection errors gracefully."""
        print("\nTesting: Database connectivity issue...")
        mock_mongo_client.side_effect = ConnectionFailure("Could not connect to MongoDB")
        load_data(EXPECTED_TRANSFORMED_DATA, "fake_uri", "fake_db", "fake_collection")
        print("  -> Passed: The function handled the exception gracefully.")

    @patch('pipeline.MongoClient')
    def test_load_data_calls_mongodb_correctly(self, mock_mongo_client):
        """Test that the load function calls MongoDB with an 'upsert' strategy."""
        print("\nTesting: Correct data upsert into MongoDB...")
        mock_db = MagicMock()
        mock_collection = MagicMock()
        mock_instance = mock_mongo_client.return_value
        mock_instance.__getitem__.return_value = mock_db
        mock_db.__getitem__.return_value = mock_collection
        
        load_data(EXPECTED_TRANSFORMED_DATA, "fake_uri", "fake_db", "cves")
        
        # Verify that replace_one is called for each record
        self.assertEqual(mock_collection.replace_one.call_count, 2)
        
        # Check the arguments of the first call to replace_one
        first_call_args = mock_collection.replace_one.call_args_list[0]
        expected_filter = {'cve_id': 'CVE-2025-0001'}
        
        # call_args is a tuple: (args, kwargs). We check both.
        self.assertEqual(first_call_args[0][0], expected_filter)  # Check filter query
        self.assertEqual(first_call_args[0][1]['cve_id'], 'CVE-2025-0001') # Check record data
        self.assertTrue(first_call_args[1]['upsert']) # Check upsert=True kwarg
        
        print("  -> Passed: Verified that replace_one() was called correctly with upsert=True.")

if __name__ == '__main__':
    unittest.main()
# tests/test_etl.py

import unittest
from unittest.mock import patch, MagicMock, call
import requests

# Import the functions we want to test
from etl import extract, transform, load

# --- MOCK DATA SAMPLES ---
# These simulate the JSON responses we expect from the SonarQube API

MOCK_PROJECTS_RESPONSE = {
    "paging": {"pageIndex": 1, "pageSize": 1, "total": 1},
    "components": [
        {"key": "project-1", "name": "Project Alpha", "qualifier": "TRK", "visibility": "public"}
    ]
}

MOCK_ISSUES_RESPONSE = {
    "paging": {"pageIndex": 1, "pageSize": 2, "total": 2},
    "issues": [
        {"key": "issue-1", "project": "project-1", "component": "c1", "message": "Bug found", "severity": "CRITICAL", "status": "OPEN", "creationDate": "2025-10-15T10:00:00+0000"},
        {"key": "issue-2", "project": "project-1", "component": "c2", "message": "Code smell", "severity": "MAJOR", "status": "CLOSED", "creationDate": "2025-10-15T11:00:00+0000"}
    ]
}

MOCK_MEASURES_RESPONSE = {
    "component": {
        "key": "project-1",
        "name": "Project Alpha",
        "measures": [
            {"metric": "bugs", "value": "5"},
            {"metric": "coverage", "value": "85.5"}
        ]
    }
}


class TestETLExtract(unittest.TestCase):
    """Tests for the data extraction module (etl/extract.py)"""

    @patch('etl.extract.session.get')
    def test_get_projects_success(self, mock_get):
        """Testing: Successful extraction of projects from the API."""
        print("\nTesting: Successful project data extraction...")
        # Configure the mock to return a successful response with our fake data
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = MOCK_PROJECTS_RESPONSE
        mock_get.return_value = mock_response

        projects = extract.get_projects()
        
        # Assertions
        self.assertEqual(len(projects), 1)
        self.assertEqual(projects[0]['key'], 'project-1')
        print("-> Passed.")

    @patch('etl.extract.session.get')
    def test_get_data_api_error(self, mock_get):
        """Testing: The extractor handles an API connection error."""
        print("\nTesting: API connection failure...")
        # Configure the mock to simulate a failed request (e.g., 403 Forbidden)
        mock_get.side_effect = requests.exceptions.HTTPError("403 Client Error")

        projects = extract.get_projects()
        
        # Assertions
        self.assertEqual(projects, []) # Should return an empty list on failure
        print("-> Passed: Handled API error gracefully.")


class TestETLTransform(unittest.TestCase):
    """Tests for the data transformation module (etl/transform.py)"""

    def test_transform_project_data(self):
        """Testing: Correct transformation of raw project data."""
        print("\nTesting: Data transformation for projects...")
        raw_projects = MOCK_PROJECTS_RESPONSE['components']
        transformed = transform.transform_project_data(raw_projects)

        # Assertions
        self.assertEqual(len(transformed), 1)
        self.assertIn('project_key', transformed[0])
        self.assertEqual(transformed[0]['name'], 'Project Alpha')
        print("-> Passed.")
        
    def test_transform_issue_data(self):
        """Testing: Correct transformation of issue data, including calculated fields."""
        print("\nTesting: Data transformation for issues...")
        raw_issues = MOCK_ISSUES_RESPONSE['issues']
        transformed = transform.transform_issue_data(raw_issues)

        # Assertions
        self.assertEqual(len(transformed), 2)
        # Test the calculated field 'is_critical_or_blocker'
        self.assertTrue(transformed[0]['is_critical_or_blocker'])  # CRITICAL issue
        self.assertFalse(transformed[1]['is_critical_or_blocker']) # MAJOR issue
        self.assertEqual(transformed[0]['severity'], 'CRITICAL')
        print("-> Passed.")

    def test_transform_measures_data(self):
        """Testing: Correct flattening of measures data."""
        print("\nTesting: Data transformation for measures...")
        raw_measures = MOCK_MEASURES_RESPONSE['component']
        transformed = transform.transform_measures_data(raw_measures)
        
        # Assertions
        self.assertEqual(transformed['project_key'], 'project-1')
        self.assertEqual(transformed['bugs'], 5.0) # Check for correct value and type conversion
        self.assertEqual(transformed['coverage'], 85.5)
        print("-> Passed.")


class TestETLLoad(unittest.TestCase):
    """Tests for the data loading module (etl/load.py)"""
    
    @patch('etl.load.db') # Mock the database object in the load module
    def test_bulk_upsert_success(self, mock_db):
        """Testing: Correct data upsert into MongoDB."""
        print("\nTesting: Correct data upsert into MongoDB...")
        # Create a mock for the collection and its bulk_write method
        mock_collection = MagicMock()
        mock_db.__getitem__.return_value = mock_collection
        
        # Sample transformed data to load
        test_data = [{'project_key': 'proj-1', 'name': 'Test Project'}]
        
        load.bulk_upsert('fake_projects', test_data, 'project_key')
        
        # Assertions
        mock_db.__getitem__.assert_called_with('fake_projects') # Check if correct collection was selected
        mock_collection.bulk_write.assert_called_once() # Check if bulk_write was called
        print("-> Passed.")
    
    @patch('etl.load.db')
    def test_bulk_upsert_database_error(self, mock_db):
        """Testing: The loader handles a database connection issue."""
        print("\nTesting: Database connectivity issue...")
        # Simulate a database error when trying to write
        mock_collection = MagicMock()
        mock_collection.bulk_write.side_effect = Exception("Could not connect to MongoDB")
        mock_db.__getitem__.return_value = mock_collection
        
        test_data = [{'project_key': 'proj-1', 'name': 'Test Project'}]

        # This should not raise an exception in the test itself, as the function should handle it
        load.bulk_upsert('fake_collection', test_data, 'project_key')
        
        # Assertions
        mock_collection.bulk_write.assert_called_once() # We check that the attempt was made
        print("-> Passed: The function handled the exception gracefully.")


if __name__ == '__main__':
    unittest.main()
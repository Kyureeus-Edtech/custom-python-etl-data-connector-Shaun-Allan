# etl/extract.py
import requests
from config import settings
import logging

# Basic session for making requests with authentication
session = requests.Session()
session.auth = (settings.SONARQUBE_API_TOKEN, '')

def fetch_paginated_data(endpoint_path, params=None):
    """
    Fetches all pages of data from a SonarQube endpoint.
    SonarQube API uses 'p' for page number and 'ps' for page size.
    """
    if params is None:
        params = {}
    
    all_results = []
    page = 1
    page_size = 100 # SonarQube's default max page size is often 500

    while True:
        params['p'] = page
        params['ps'] = page_size
        
        url = f"{settings.SONARQUBE_URL}{endpoint_path}"
        try:
            response = session.get(url, params=params)
            response.raise_for_status()  # Raises an exception for bad status codes (4xx or 5xx)
            data = response.json()
            
            # Find the main data list in the response (e.g., 'projects', 'issues', 'components')
            key = next((k for k in ['components', 'issues', 'projects', 'hotspots'] if k in data), None)
            if not key or not data[key]:
                break # No more data to fetch

            all_results.extend(data[key])
            
            # Check if we are on the last page
            total = data.get('paging', {}).get('total', 0)
            if (page * page_size) >= total:
                break
            
            page += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from {url}: {e}")
            break
            
    return all_results

def get_projects():
    """Extracts all projects from SonarQube."""
    logging.info("Extracting projects...")
    return fetch_paginated_data("/api/projects/search")

def get_issues_for_project(project_key):
    """Extracts all issues for a given project."""
    logging.info(f"Extracting issues for project: {project_key}")
    params = {"componentKeys": project_key}
    return fetch_paginated_data("/api/issues/search", params=params)

def get_measures_for_project(project_key):
    """Extracts key measures for a given project."""
    logging.info(f"Extracting measures for project: {project_key}")
    metric_keys = "bugs,vulnerabilities,code_smells,coverage,duplicated_lines_density"
    params = {"component": project_key, "metricKeys": metric_keys}
    url = f"{settings.SONARQUBE_URL}/api/measures/component"
    try:
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json().get('component', {})
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching measures for {project_key}: {e}")
        return None
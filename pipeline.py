import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from tenacity import retry, stop_after_attempt, wait_exponential

#################################
#            EXTRACT            #
#################################

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30))
def extract_nvd_data(base_url, api_key=None, days=30):
    """
    Extracts CVE data from the NVD API for the last specified number of days.
    """
    print(f"Extracting CVEs modified in the last {days} days...")
    
    # Calculate date range
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=days)
    
    # Format dates for the API
    start_date_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    headers = {}
    if api_key:
        headers['apiKey'] = api_key
        print("Using API key for request.")

    params = {
        'pubStartDate': start_date_str,
        'pubEndDate': end_date_str,
        'resultsPerPage': 2000 # Max allowed results per page
    }

    try:
        response = requests.get(base_url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        
        vulnerabilities = data.get('vulnerabilities', [])
        print(f"Successfully extracted {len(vulnerabilities)} CVE records.")
        return vulnerabilities

    except requests.exceptions.RequestException as e:
        print(f"Failed to get NVD data due to an error: {e}")
        raise e

#################################
#           TRANSFORM           #
#################################

def transform_nvd_data(records):
    """
    Transforms the raw, nested CVE data from NVD into a flat structure.
    """
    if not records:
        return []
    
    print(f"Transforming {len(records)} records...")
    transformed_records = []

    for item in records:
        cve = item.get('cve', {})
        cve_id = cve.get('id')
        
        # Extract description (English)
        description = next((d['value'] for d in cve.get('descriptions', []) if d['lang'] == 'en'), None)
        
        # Extract CVSS v3.1 metrics (preferred)
        metrics = cve.get('metrics', {})
        cvss_data = metrics.get('cvssMetricV31', [{}])[0].get('cvssData', {})
        
        clean_record = {
            "cve_id": cve_id,
            "published_date": cve.get('published'),
            "last_modified_date": cve.get('lastModified'),
            "description": description,
            "base_score": cvss_data.get('baseScore'),
            "severity": cvss_data.get('baseSeverity'),
            "vector_string": cvss_data.get('vectorString'),
            "ingestion_timestamp": datetime.now(timezone.utc).isoformat()
        }
        transformed_records.append(clean_record)
            
    print("Transformation complete.")
    return transformed_records

#################################
#             LOAD              #
#################################

def load_data(data, mongo_uri, db_name, collection_name):
    """
    Loads transformed CVE data into MongoDB using an 'upsert' strategy.
    This inserts new CVEs and updates existing ones if they have changed.
    """
    if not data:
        print("No data to load.")
        return

    print(f"Connecting to MongoDB to load data into '{collection_name}'...")
    client = None
    upsert_count = 0
    try:
        client = MongoClient(mongo_uri)
        client.admin.command('ismaster')
        db = client[db_name]
        collection = db[collection_name]
        
        for record in data:
            # Use the cve_id as the unique key for matching documents
            filter_query = {"cve_id": record["cve_id"]}
            
            # replace_one with upsert=True will update the document if it exists,
            # or insert it if it does not.
            result = collection.replace_one(filter_query, record, upsert=True)
            if result.upserted_id:
                upsert_count += 1
        
        print(f"Successfully upserted {upsert_count} documents into '{db_name}.{collection_name}'.")

    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")
    except Exception as e:
        print(f"An error occurred during loading: {e}")
    finally:
        if client:
            client.close()
            print("MongoDB connection closed.")
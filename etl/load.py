# etl/load.py
from pymongo import MongoClient, UpdateOne
from config import settings
import logging

# Establish a connection to MongoDB
client = MongoClient(settings.MONGO_URI)
db = client[settings.MONGO_DATABASE]

def bulk_upsert(collection_name, data, id_key):
    """
    Performs a bulk 'upsert' operation in MongoDB.
    - Updates a document if it exists (based on id_key).
    - Inserts a new document if it does not exist.
    """
    if not data:
        logging.warning(f"No data provided to load into '{collection_name}'.")
        return

    collection = db[collection_name]
    
    operations = [
        UpdateOne(
            {id_key: item[id_key]},  # The filter to find the document
            {"$set": item},          # The data to update/insert
            upsert=True              # The upsert option
        )
        for item in data
    ]
    
    try:
        logging.info(f"Performing bulk upsert on {len(data)} items into '{collection_name}'...")
        result = collection.bulk_write(operations)
        logging.info(f"Bulk write to '{collection_name}' complete. Upserted: {result.upserted_count}, Modified: {result.modified_count}")
    except Exception as e:
        logging.error(f"Error during bulk write to '{collection_name}': {e}")
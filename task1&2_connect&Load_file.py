from pymongo import MongoClient
from bson import json_util
import os

# Establish a connection to the MongoDB database.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']  # Access the specified database.

def bulk_load_data(db, directory):
    for file_name in os.listdir(directory):
        if file_name.endswith('.json'):
            collection_name = os.path.splitext(file_name)[0]  # Extract collection name from file name
            
            # Create collection if it doesn't exist
            if collection_name not in db.list_collection_names():
                db.create_collection(collection_name)
                print(f"Collection '{collection_name}' created.")

            # Read JSON file line by line and insert each line into collection
            with open(os.path.join(directory, file_name), 'r') as f:
                for line in f:
                    # Convert JSON data to BSON
                    data = json_util.loads(line)
                    # Insert data into collection
                    db[collection_name].insert_one(data)
                print(f"Data inserted into '{collection_name}' collection.")



# Call the function to load data into collections
bulk_load_data(db, '/Users/sonamkumari/Desktop/helloWorld/MongoDB/sample_mflix')

print("Successfully Loaded in Collections!")  # Print a confirmation message.

client.close()  # Close the MongoDB connection.

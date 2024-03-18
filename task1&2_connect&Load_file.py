from pymongo import MongoClient
import json

# Establish a connection to the MongoDB database.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']  # Access the specified database.

# Function to load JSON data from a file into a specified MongoDB collection.
def bulk_load_json_file(collection_name, json_file_path):
    collection = db[collection_name]  # Access the collection within the database.
    with open(json_file_path, 'r') as file:  # Open the JSON file for reading.
        data = json.load(file)  # Load the JSON data from the file.
        for x in data:  # Iterate over the items in the JSON data.
            collection.insert_one(x)  # Insert each item into the collection.

# List of collection names to be populated with data.
collection_list = ['comments', 'movies', 'theaters', 'users']
path_name = '/Users/sonamkumari/Desktop/helloWorld/MongoDB/sample_mflix'  # Base path where the JSON files are located.
extension = '.json'  # File extension for the JSON files.

# Iterate over each collection name and load the corresponding JSON file.
for each_file in collection_list:
    full_path = path_name + "/" + each_file + extension  # Construct the full file path.
    bulk_load_json_file(each_file, full_path)  # Load the JSON file into the collection.

print("Successfully Loaded in Collections!")  # Print a confirmation message.

client.close()  # Close the MongoDB connection.

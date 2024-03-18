from pymongo import MongoClient

# Set up MongoDB connection.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']
theaters_collection = db.theaters

def find_top_cities_with_theaters():
    # Aggregate to count theaters per city and get the top 10.
    aggregation_pipeline = [
        {"$group": {"_id": "$location.address.city", "theaterTotal": {"$sum": 1}}},
        {"$sort": {"theaterTotal": -1}},
        {"$limit": 10}
    ]
    return list(theaters_collection.aggregate(aggregation_pipeline))

# Get and display the top 10 cities by theater count.
top_theater_cities = find_top_cities_with_theaters()
print("\nTop 10 Cities with the Most Theaters:")
for idx, city_info in enumerate(top_theater_cities, start=1):
    print(f"{idx}. {city_info['_id']}: {city_info['theaterTotal']} theaters")

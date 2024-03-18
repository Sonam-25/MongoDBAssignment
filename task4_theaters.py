from pymongo import MongoClient

# Set up MongoDB connection.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']
theaters_collection = db.theaters

#1. Top 10 cities with the maximum number of theaters
def find_top_cities_with_theaters():
    # Aggregate to count theaters per city and get the top 10.
    aggregation_pipeline = [
        {"$group": {"_id": "$location.address.city", "theaterTotal": {"$sum": 1}}},
        {"$sort": {"theaterTotal": -1}},
        {"$limit": 10}
    ]
    return list(theaters_collection.aggregate(aggregation_pipeline))

top_theater_cities = find_top_cities_with_theaters()
print("\n1. Top 10 Cities with the Most Theaters:")
for idx, city_info in enumerate(top_theater_cities, start=1):
    print(f"{idx}. {city_info['_id']}: {city_info['theaterTotal']} theaters")


#2. Top 10 theatres nearby given coordinates
theaters_collection.create_index([("location.geo", "2dsphere")])

def top_n_theatres_nearby(coordinates, n):

    # Find theatres near given coordinates
    query = {
        "location.geo": {
            "$nearSphere": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": coordinates
                },
                "$maxDistance": 5000
            }
        }
    }
    
    # Limit the result to top N theatres
    result = theaters_collection.find(query).limit(n)
    
    # Return the result
    return list(result)

# Example usage:
coordinates = [-93.24565, 44.85466]  # Example coordinates
n = 10  # Number of theatres to retrieve
top_theatres = top_n_theatres_nearby(coordinates, n)
print("\n2. Top 10 theaters nearby the given coordinates (130,40): \n")
for theatre in top_theatres:
    print("Theater with theaterId:",theatre['theaterId'],"city: ",theatre['location']['address']['city'])

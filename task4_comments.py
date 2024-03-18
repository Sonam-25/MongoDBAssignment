from pymongo import MongoClient
from datetime import datetime

# Initialize MongoDB connection.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']

# Aggregate top 10 users by comment count.
top_users = db.comments.aggregate([
    {"$group": {
        "_id": "$email",  # Group by email to identify unique users.
        "count": {"$sum": 1}  # Sum the number of comments per user.
    }},
    {"$sort": {"count": -1}},  # Sort the results by comment count in descending order.
    {"$limit": 10}  # Limit the results to the top 10.
])

# Aggregate top 10 movies by comment count.
top_movies = db.comments.aggregate([
    {"$group": {
        "_id": "$movie_id",  # Group by movie_id to identify unique movies.
        "count": {"$sum": 1}  # Sum the number of comments per movie.
    }},
    {"$sort": {"count": -1}},  # Sort the results by comment count in descending order.
    {"$limit": 10}  # Limit the results to the top 10.
])

# Function to break down the number of comments created each month in a given year.
def yearly_comments_breakdown(year):
    # Calculate the start and end of the specified year.
    start_of_year = datetime(year, 1, 1)
    end_of_year = datetime(year + 1, 1, 1)

    # Convert start and end dates to Unix timestamps for comparison.
    start_unix_timestamp = int(start_of_year.timestamp())
    end_unix_timestamp = int(end_of_year.timestamp())

    # Aggregation pipeline to group and count comments by month.
    pipeline = [
        {"$match": {
            "date.$date.$numberLong": {"$gte": str(start_unix_timestamp), "$lt": str(end_unix_timestamp)}
        }},
        {"$group": {
            "_id": {"month": "$month", "year": "$year"},
            "total_comments": {"$sum": 1}
        }},
        {"$project": {
            "_id": 0,
            "month": "$_id.month",
            "year": "$_id.year",
            "total_comments": 1
        }}
    ]

    # Execute pipeline and return the results.
    result = db.comments.aggregate(pipeline)
    return list(result)

# Display top 10 users by comment count.
print("\nTop 10 users who made the maximum number of comments:")
for user in top_users:
    print(user)

# Fetch movie titles for the top commented movies and display them.
final_movie_result = []
for movie in top_movies:
    id = movie.get("_id").get('$oid')
    movie_name = db.movies.find_one({"_id.oid": id}, {'title': 1, '_id': 0})
    count = movie.get("count")
    final_movie_result.append((movie_name['title'], count))

print("\nTop 10 movies with most comments:")
for movie in final_movie_result:
    print(movie)

# Display the number of comments created each month in a given year.
year = 2009
print(f"\nThe total number of comments created each month in {year}:")
total_comments_by_month = yearly_comments_breakdown(year)
for month_data in total_comments_by_month:
    print(month_data)

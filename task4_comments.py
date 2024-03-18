from pymongo import MongoClient
from datetime import datetime

# Initialize MongoDB connection.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']

# 1. Find top 10 users who made the maximum number of comments
top_users = db.comments.aggregate([
    {"$group": {
        "_id": "$email",  # Group by email to identify unique users.
        "count": {"$sum": 1}  # Sum the number of comments per user.
    }},
    {"$sort": {"count": -1}},  # Sort the results by comment count in descending order.
    {"$limit": 10}  # Limit the results to the top 10.
])
# Display top 10 users by comment count.
print("\nTop 10 users who made the maximum number of comments:")
for user in top_users:
    print(user)

# 2. Find top 10 movies with most comments
top_movies = db.comments.aggregate([
    {"$group": {
        "_id": "$movie_id",  # Group by movie_id to identify unique movies.
        "count": {"$sum": 1}  # Sum the number of comments per movie.
    }},
    {"$sort": {"count": -1}},  # Sort the results by comment count in descending order.
    {"$limit": 10}  # Limit the results to the top 10.
])

# Fetch movie titles for the top commented movies and display them.
final_movie_result = []
for movie in top_movies:
    movie_details = db.movies.find_one({"_id": movie['_id']})
    if movie_details:
        final_movie_result.append({"title": movie_details['title'], "comment_count": movie['count']})


print("\nTop 10 movies with most comments:")
for movie in final_movie_result:
    print(movie)

#3. Given a year find the total number of comments created each month in that year

given_year = 1998  # Update with the desired year

# Aggregate to find total number of comments created each month in the given year
pipeline = [
    {"$match": {"date": {"$gte": datetime(given_year, 1, 1), "$lt": datetime(given_year + 1, 1, 1)}}},
    {"$project": {"month": {"$month": "$date"}}},
    {"$group": {"_id": "$month", "total_comments": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]

comments_by_month = list(db.comments.aggregate(pipeline))

# Print the total number of comments created each month in the given year
print(f"\nTotal number of comments created in each month in {given_year}:")
for month_data in comments_by_month:
    month_number = month_data['_id']
    month_name = datetime(1900, month_number, 1).strftime('%B')  # Get month name from month number
    total_comments = month_data['total_comments']
    print(f"{month_name} {given_year}: {total_comments} comments")



'''
task 3 : Create Python methods and MongoDB queries to insert new comments, movies, theatres,
and users into respective MongoDB collections.
'''
from pymongo import MongoClient

# Establish a connection to MongoDB.
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host, mongodb_port)
db = client['MongoDbAssignment']

# Sample data for various collections.
comment_comments = {
    "name": "Emma Watson",
    "email": "emma.doe@gmail.com",
    "movie_id": "some_movie_id",
    "text": "I liked it!",
    "date": "2024-10-10"
}

comment_movies = {
    "plot": "Three men hammer on an anvil and pass a bottle of beer around.",
    "genres": ["Short", "Comedy"],
    "runtime": 12,
    "num_mflix_comments": 10,
    "title": "Johnsmith Scene",
    "released": {"$date": {"$numberLong": "-2418768000000"}},
    "rated": "RATED",
    "lastupdated": "2015-10-26 00:03:50.133000000",
    "year": 1893,
    "type": "movie",
}

comment_theaters = {
    "name": "Spring Mall",
    "city": "Ranchi",
    "capacity": 260
}

comment_users = {
    "name": "Sonam",
    "email": "sonam@reddif.com",
    "age": 23
}

# Function definitions for inserting data into each collection.
def insert_comment(data):
    """Inserts a single comment into the 'comments' collection."""
    db['comments'].insert_one(data)

def insert_movie(data):
    """Inserts a single movie into the 'movies' collection."""
    db['movies'].insert_one(data)

def insert_theater(data):
    """Inserts a single theater into the 'theaters' collection."""
    db['theaters'].insert_one(data)

def insert_user(data):
    """Inserts a single user into the 'users' collection."""
    db['users'].insert_one(data)

# Insert the sample data into their respective collections.
insert_comment(comment_comments)
insert_movie(comment_movies)
insert_theater(comment_theaters)
insert_user(comment_users)

print("\nData Inserted Successfully!")

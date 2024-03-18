from pymongo import MongoClient

#Connecting MongoDb
mongodb_host = 'localhost'
mongodb_port = 27017
client = MongoClient(mongodb_host,mongodb_port)
db = client['MongoDbAssignment'] 

#i) 1. Find N movies with the highest IMD Rating
def fetch_highest_rated_movies(limit):
    result = db.movies.find({}, {"title": 1, "imdb.rating": 1, "_id": 0}).sort("imdb.rating", -1).limit(limit)
    movies_list = [(movie["title"], movie["imdb"]["rating"]) for movie in result]
    return movies_list

movies_list = fetch_highest_rated_movies(10)
print("\nTop 10 Highest Rated Movies:")
for title, rating in movies_list:
    print(f"{title}: {rating}")


#i) 2. Find N movies with the highest IMD Rating in a given year
def highest_rated_movies_yearly(limit, target_year):
    match_stage = {"$match": {"year":target_year}}
    project_stage = {"$project": {"title": 1, "imdb.rating": 1, "_id": 0}}
    sort_stage = {"$sort": {"imdb.rating": -1}}
    pipeline = [match_stage, project_stage, sort_stage, {"$limit": limit}]
    movies = db.movies.aggregate(pipeline)
    return [(movie["title"], movie["imdb"]["rating"]) for movie in movies]

year= 1915
movies_yearly = highest_rated_movies_yearly(10, year)
print("\nTop 10 Highest Rated Movies in 1915:")
for title, rating in movies_yearly:
    print(f"{title}: {rating}")

if(len(movies_yearly) <10):
    print(f"\nOnly {len(movies_yearly)} available in {year}")


# i) 3. with highest IMDB rating with number of votes > 1000
def top_rated_movies_with_votes(N,votes_threshold):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": votes_threshold}}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": N}
    ]
    top_rated_movie = db.movies.aggregate(pipeline)
    movie_info = []
    for movie in top_rated_movie:
        title = movie.get("title")
        rating = movie.get("imdb.rating")
        movie_info.append((title, rating))
    
    return  movie_info


top_voted_movie=top_rated_movies_with_votes(5,1000)
print("\nTop movies with highest IMDB rating with number of votes > 1000 ")
for movie in top_voted_movie:
    print(movie)

#i) 4. with title matching a given pattern sorted by highest tomatoes ratings

def movies_matching_title_sorted_by_rating(limit, pattern):
    regex_match = {"$match": {"title": {"$regex": pattern, "$options": "i"}}}
    sort_stage = {"$sort": {"tomatoes.viewer.rating": -1}}
    pipeline = [regex_match, sort_stage, {"$limit": limit}]
    result = db.movies.aggregate(pipeline)
    return [(movie["title"], movie["tomatoes"]["viewer"]["rating"]) for movie in result]

movies_title_pattern = movies_matching_title_sorted_by_rating(5, "Star")
print("\nMovies Matching 'Star' Sorted by Tomatoes Rating:")
for title, rating in movies_title_pattern:
    print(f"{title}: {rating}")

# ii) 1. Find top `N` directors -who created the maximum number of movies

def directors_with_most_films(limit):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "totalMovies": {"$sum": 1}}},
        {"$sort": {"totalMovies": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))

directors_most_films = directors_with_most_films(5)
print("\nTop 5 Directors with Most Films:")
for director in directors_most_films:
    print(f"{director['_id']}: {director['totalMovies']} movies")

# ii) 2. Find top `N` directors -who created the maximum number of movies in a given year

# Aggregate and list directors with the most films in a given year.
def directors_most_films_year(limit, year):
    # Match by year, then follow the same aggregation logic as above.
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "totalMovies": {"$sum": 1}}},
        {"$sort": {"totalMovies": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))


directors_year = directors_most_films_year(5, 1915)
print("\nTop 5 Directors with Most Films in 1915:")
for director in directors_year:
    print(f"{director['_id']}: {director['totalMovies']} movies")


#ii) 3. Find top `N` directors- who created the maximum number of movies for a given genre
def directors_most_films_genre(limit, genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$directors"},
        {"$group": {"_id": "$directors", "totalMovies": {"$sum": 1}}},
        {"$sort": {"totalMovies": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))


directors_genre = directors_most_films_genre(5, "Drama")
print("\nTop 5 Directors with Most Drama Films:")
for director in directors_genre:
    print(f"{director['_id']}: {director['totalMovies']} movies")

#iii) 1. Find top `N` actors - who starred in the maximum number of movies
def actors_most_roles(limit):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "totalRoles": {"$sum": 1}}},
        {"$sort": {"totalRoles": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))

actors_roles = actors_most_roles(5)
print("\nTop 5 Actors with Most Roles:")
for actor in actors_roles:
    print(f"{actor['_id']}: {actor['totalRoles']} roles")

#iii) 2. Find top `N` actors - who starred in the maximum number of movies in a given year

def actors_most_roles_year(limit, year):
    pipeline = [
        {"$match": {"year": year}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "totalRoles": {"$sum": 1}}},
        {"$sort": {"totalRoles": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))

actors_year = actors_most_roles_year(5, 2000)
print("\nTop 5 Actors with Most Roles in 2000:")
for actor in actors_year:
    print(f"{actor['_id']}: {actor['totalRoles']} roles")

#iii) 3. Find top `N` actors - who starred in the maximum number of movies for a given genre

def actors_most_roles_genre(limit, genre):
    pipeline = [
        {"$match": {"genres": genre}},
        {"$unwind": "$cast"},
        {"$group": {"_id": "$cast", "totalRoles": {"$sum": 1}}},
        {"$sort": {"totalRoles": -1}},
        {"$limit": limit}
    ]
    return list(db.movies.aggregate(pipeline))

actors_genre = actors_most_roles_genre(5, "Comedy")
print("\nTop 5 Actors with Most Comedy Roles:")
for actor in actors_genre:
    print(f"{actor['_id']}: {actor['totalRoles']} roles")

#4. Find top `N` movies for each genre with the highest IMDB rating

def genre_top_movies(limit):

    #sorting according to genre
    pipeline = [
        {"$unwind": "$genres"},
        {"$sort": {"imdb.rating": -1}},
        {"$group": {"_id": "$genres", "topMovies": {"$push": {"title": "$title", "rating": "$imdb.rating"}}}},
        {"$project": {"_id": 0, "genre": "$_id", "topMovies": {"$slice": ["$topMovies", limit]}}}
    ]
    return list(db.movies.aggregate(pipeline))

#priniting top 3 movies of each genre
genre_movies = genre_top_movies(3)
print("\nTop 3 Movies for Each Genre:")
for genre_info in genre_movies:
    print(f"Genre: {genre_info['genre']}")
    for movie in genre_info['topMovies']:
        try:
            if(movie['rating']==""):
                print(f" - {movie['title']}: 'rating missing'")
            else:
                print(f" - {movie['title']}: {movie['rating']}")
        except Exception as e:
            print(f" No rating exist for- {movie['title']}")
    print()
















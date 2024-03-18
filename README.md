# MongoDBAssignment
Tasks:-

1. Create a Python application to connect to MongoDB.

2. Bulk load the JSON files in the individual MongoDB collections using Python. MongoDB collections -
comments
movies
theaters
users

3. Create Python methods and MongoDB queries to insert new comments, movies, theatres, and users into respective MongoDB collections.

4. Create Python methods and MongoDB queries to support the below operations -
a) comments collection
i. Find top 10 users who made the maximum number of comments
ii. Find top 10 movies with most comments
iii. Given a year find the total number of comments created each month in that year

b) movies collection
i) Find top `N` movies - 
1. with the highest IMDB rating
2. with the highest IMDB rating in a given year
3. with highest IMDB rating with number of votes > 1000
4. with title matching a given pattern sorted by highest tomatoes ratings

ii) Find top `N` directors -
1. who created the maximum number of movies
2. who created the maximum number of movies in a given year
3. who created the maximum number of movies for a given genre

iii) Find top `N` actors - 
1. who starred in the maximum number of movies
2. who starred in the maximum number of movies in a given year
3. who starred in the maximum number of movies for a given genre
4. Find top `N` movies for each genre with the highest IMDB rating

c) theatre collection
1. Top 10 cities with the maximum number of theatres
2. top 10 theatres nearby given coordinates

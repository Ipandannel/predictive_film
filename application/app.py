from flask import Flask, jsonify, request, render_template
import mysql.connector
import os

app = Flask(__name__)

def get_db_connection():
    """Establishes a connection to the database."""
    try:
        connection = mysql.connector.connect(
            host=os.environ.get("DATABASE_HOST", "database"),
            user=os.environ.get("DATABASE_USER", "root"),
            password=os.environ.get("DATABASE_PASSWORD", "example"),
            database=os.environ.get("DATABASE_NAME", "moviedb"),
            connection_timeout=5
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
        return None

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/search", methods=["GET"])
def search_movies():
    """Search for movies based on filters."""
    query = request.args.get("q", "").strip()
    genre = request.args.get("genre", "").strip()
    min_rating = request.args.get("min_rating", "").strip()
    max_rating = request.args.get("max_rating", "").strip()
    director = request.args.get("director", "").strip()
    actor = request.args.get("actor", "").strip()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            movies.movieId, movies.title, 
            IFNULL(GROUP_CONCAT(DISTINCT genres.genre_name SEPARATOR ', '), 'Unknown') AS genre,
            IFNULL(movies.avg_rating, 0) AS avg_rating,
            IFNULL(movies.release_date, 'Unknown') AS release_date,
            IFNULL(movies.poster_url, '') AS poster_url,
            IFNULL(GROUP_CONCAT(DISTINCT directors.director_name SEPARATOR ', '), 'Unknown') AS directors,
            IFNULL(GROUP_CONCAT(DISTINCT actors.actor_name SEPARATOR ', '), 'Unknown') AS actors,
            IFNULL(ratings.imdb_rating, 0) AS imdb_rating,
            IFNULL(ratings.rotten_tomatoes, 0) AS rt_score,
            IFNULL(awards.oscars_won, 0) AS oscars,
            IFNULL(awards.golden_globes_won, 0) AS golden_globes,
            IFNULL(awards.baftas_won, 0) AS baftas
        FROM movies
        LEFT JOIN movie_genres ON movies.movieId = movie_genres.movieId
        LEFT JOIN genres ON movie_genres.genreId = genres.id
        LEFT JOIN movie_directors ON movies.movieId = movie_directors.movieId
        LEFT JOIN directors ON movie_directors.director_id = directors.id
        LEFT JOIN movie_actors ON movies.movieId = movie_actors.movieId
        LEFT JOIN actors ON movie_actors.actor_id = actors.id
        LEFT JOIN ratings ON movies.movieId = ratings.movieId
        LEFT JOIN awards ON movies.movieId = awards.movieId
        WHERE 1=1
    """
    query_params = []

    # 🔎 Filter by title
    if query:
        sql_query += " AND LOWER(movies.title) LIKE LOWER(%s)"
        query_params.append(f"%{query}%")

    # 🔎 Filter by genre
    if genre:
        sql_query += " AND genres.genre_name = %s"
        query_params.append(genre)

    # 🔎 Filter by director
    if director:
        sql_query += " AND directors.director_name LIKE %s"
        query_params.append(f"%{director}%")

    # 🔎 Filter by lead actor
    if actor:
        sql_query += " AND actors.actor_name LIKE %s"
        query_params.append(f"%{actor}%")

    # 🔎 Filter by rating range
    if min_rating:
        sql_query += " AND movies.avg_rating >= %s"
        query_params.append(float(min_rating))
    if max_rating:
        sql_query += " AND movies.avg_rating <= %s"
        query_params.append(float(max_rating))

    sql_query += " GROUP BY movies.movieId LIMIT 20;"  # Avoid duplicates

    cursor.execute(sql_query, tuple(query_params))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()
    
    return jsonify(movies)

@app.route("/movies")
def get_movies():
    """Fetch all movies with details."""
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT movies.movieId, movies.title, 
               IFNULL(avg_rating, 0) AS avg_rating, 
               IFNULL(release_date, 'Unknown') AS release_date,
               IFNULL(poster_url, '') AS poster_url
        FROM movies;
    """)
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(movies)

@app.route("/genres", methods=["GET"])
def get_genres():
    """Fetch unique genres."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT DISTINCT genre_name FROM genres ORDER BY genre_name;")
    genres = [row["genre_name"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(genres)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)


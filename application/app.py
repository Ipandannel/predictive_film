from flask import Flask, jsonify, request, render_template
import mysql.connector
import os

app = Flask(__name__)

def get_db_connection():
    try:
        connection = mysql.connector.connect(
            host=os.environ.get("DATABASE_HOST", "database"),
            user=os.environ.get("DATABASE_USER", "root"),
            password=os.environ.get("DATABASE_PASSWORD", "example"),
            database=os.environ.get("DATABASE_NAME", "moviedb"),
            connection_timeout=5  # Avoids hanging if the DB is down
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Database connection failed: {err}")
        return None


@app.route("/")
def index():
    return render_template("index.html")  # Serve the frontend

@app.route("/search", methods=["GET"])
def search_movies():
    query = request.args.get("q", "").strip()
    genre = request.args.get("genre", "").strip()
    min_rating = request.args.get("min_rating", "").strip()
    max_rating = request.args.get("max_rating", "").strip()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = """
        SELECT movies.title, 
               IFNULL(GROUP_CONCAT(DISTINCT genres.genre SEPARATOR ', '), 'Unknown') AS genre,
               IFNULL(movies.avg_rating, 0) AS avg_rating
        FROM movies
        LEFT JOIN movie_genres ON movies.movieId = movie_genres.movieId
        LEFT JOIN genres ON movie_genres.genreId = genres.id
        WHERE 1=1
    """
    query_params = []

    # Filter by title
    if query:
        sql_query += " AND LOWER(movies.title) LIKE LOWER(%s)"
        query_params.append(f"%{query}%")

    # Filter by genre
    if genre:
        sql_query += " AND genres.genre = %s"
        query_params.append(genre)

    # Filter by rating range
    if min_rating:
        sql_query += " AND movies.avg_rating >= %s"
        query_params.append(float(min_rating))
    if max_rating:
        sql_query += " AND movies.avg_rating <= %s"
        query_params.append(float(max_rating))

    sql_query += " GROUP BY movies.movieId LIMIT 10;"

    cursor.execute(sql_query, tuple(query_params))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(movies)


@app.route("/movies")
def get_movies():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT movieId, title, IFNULL(avg_rating, 0) AS avg_rating FROM movies;")
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(movies)

@app.route("/genres", methods=["GET"])
def get_genres():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT DISTINCT genre FROM genres ORDER BY genre;")
    genres = [row["genre"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(genres)

if __name__ == "__main__":
    # Make sure the Flask app listens on all interfaces so it can be reached from outside the container.
    app.run(host="0.0.0.0", port=5000)

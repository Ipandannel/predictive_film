from flask import Flask, jsonify, request, render_template, session, redirect, url_for
import mysql.connector
import os
import bcrypt 
from flask_session import Session
app = Flask(__name__)
app.config['SECRET_KEY'] = 'my_secret_key'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)
print("Secret Key:", app.config['SECRET_KEY'])

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
    """Search for movies based on filters with pagination."""
    query = request.args.get("q", "").strip()
    genres = request.args.get("genres", "").strip().split(",") if request.args.get("genres") else []
    min_rating = request.args.get("min_rating", "").strip()
    max_rating = request.args.get("max_rating", "").strip()
    director = request.args.get("director", "").strip()
    actor = request.args.get("actor", "").strip()
    release_date_from = request.args.get("releaseDateFrom", "").strip()
    release_date_to = request.args.get("releaseDateTo", "").strip()
    min_runtime = request.args.get("min_runtime", "").strip()
    max_runtime = request.args.get("max_runtime", "").strip()
    language = request.args.get("language", "").strip()
    min_oscars = request.args.get("minOscars", "").strip()
    min_golden_globes = request.args.get("minGoldenGlobes", "").strip()
    min_baftas = request.args.get("minBAFTAs", "").strip()
    
    # Pagination parameters
    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    try:
        page_size = int(request.args.get("page_size", 20))
    except ValueError:
        page_size = 20
    offset = (page - 1) * page_size

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Build a common base query with all filters
    base_query = """
        FROM movies
        LEFT JOIN movie_genres ON movies.movieId = movie_genres.movieId
        LEFT JOIN genres ON movie_genres.genreId = genres.id
        LEFT JOIN movie_directors ON movies.movieId = movie_directors.movieId
        LEFT JOIN directors ON movie_directors.director_id = directors.id
        LEFT JOIN movie_actors ON movies.movieId = movie_actors.movieId
        LEFT JOIN actors ON movie_actors.actor_id = actors.id
        LEFT JOIN ratings ON movies.movieId = ratings.movieId
        LEFT JOIN awards ON movies.movieId = awards.movieId
        LEFT JOIN languages ON movies.language_id = languages.id
        WHERE 1=1
    """
    query_params = []

    # Filter by title
    if query:
        base_query += " AND LOWER(movies.title) LIKE LOWER(%s)"
        query_params.append(f"%{query}%")

    # Filter by genres
    if genres:
        base_query += """
            AND movies.movieId IN (
                SELECT mg.movieId FROM movie_genres mg 
                JOIN genres g ON mg.genreId = g.id 
                WHERE g.genre_name IN ({})
                GROUP BY mg.movieId
                HAVING COUNT(DISTINCT g.genre_name) = %s
            )
        """.format(", ".join(["%s"] * len(genres)))
        query_params.extend(genres)
        query_params.append(len(genres))

    # Filter by director
    if director:
        base_query += """
            AND movies.movieId IN (
                SELECT md.movieId FROM movie_directors md 
                JOIN directors d ON md.director_id = d.id 
                WHERE d.director_name LIKE %s
            )
        """
        query_params.append(f"%{director}%")

    # Filter by actor
    if actor:
        base_query += """
            AND movies.movieId IN (
                SELECT ma.movieId FROM movie_actors ma 
                JOIN actors a ON ma.actor_id = a.id 
                WHERE a.actor_name LIKE %s
            )
        """
        query_params.append(f"%{actor}%")

    # Filter by rating
    if min_rating:
        base_query += " AND movies.avg_rating >= %s"
        query_params.append(float(min_rating))
    if max_rating:
        base_query += " AND movies.avg_rating <= %s"
        query_params.append(float(max_rating))

    # Filter by release date
    if release_date_from:
        base_query += " AND movies.release_date >= %s"
        query_params.append(release_date_from)
    if release_date_to:
        base_query += " AND movies.release_date <= %s"
        query_params.append(release_date_to)

    # Filter by runtime
    if min_runtime:
        base_query += " AND CAST(SUBSTRING_INDEX(movies.runtime, ' ', 1) AS UNSIGNED) >= %s"
        query_params.append(int(min_runtime))
    if max_runtime:
        base_query += " AND CAST(SUBSTRING_INDEX(movies.runtime, ' ', 1) AS UNSIGNED) <= %s"
        query_params.append(int(max_runtime))

    if language:
        languages_list = [l.strip() for l in language.split(",") if l.strip()]
        if len(languages_list) == 1:
            base_query += " AND languages.language_name = %s"
            query_params.append(languages_list[0])
        else:
            placeholders = ", ".join(["%s"] * len(languages_list))
            base_query += f" AND languages.language_name IN ({placeholders})"
            query_params.extend(languages_list)

    # Filter by awards
    if min_oscars:
        base_query += " AND awards.oscars_won >= %s"
        query_params.append(int(min_oscars))
    if min_golden_globes:
        base_query += " AND awards.golden_globes_won >= %s"
        query_params.append(int(min_golden_globes))
    if min_baftas:
        base_query += " AND awards.baftas_won >= %s"
        query_params.append(int(min_baftas))

    # --- Count Query: Get total matching movies ---
    count_query = "SELECT COUNT(DISTINCT movies.movieId) AS total " + base_query
    cursor.execute(count_query, tuple(query_params))
    count_result = cursor.fetchone()
    total = count_result["total"] if count_result and "total" in count_result else 0

    # --- Main Query: Fetch movie data with pagination ---
    main_query = """
        SELECT 
            movies.movieId, movies.title, 
            IFNULL(GROUP_CONCAT(DISTINCT genres.genre_name SEPARATOR ', '), 'Unknown') AS genre,
            IFNULL(movies.avg_rating, 0) AS avg_rating,
            IFNULL(movies.release_date, 'Unknown') AS release_date,
            IFNULL(movies.poster_url, '') AS poster_url,
            IFNULL(GROUP_CONCAT(DISTINCT directors.director_name SEPARATOR ', '), 'Unknown') AS directors,
            IFNULL(GROUP_CONCAT(DISTINCT actors.actor_name SEPARATOR ', '), 'Unknown') AS actors,
            IFNULL(movies.runtime, 'Unknown') AS runtime,
            IFNULL(languages.language_name, 'Unknown') AS language,
            IFNULL(ratings.imdb_rating, 0) AS imdb_rating,
            IFNULL(ratings.rotten_tomatoes, 0) AS rt_score,
            IFNULL(awards.oscars_won, 0) AS oscars,
            IFNULL(awards.golden_globes_won, 0) AS golden_globes,
            IFNULL(awards.baftas_won, 0) AS baftas
    """ + base_query + " GROUP BY movies.movieId LIMIT %s OFFSET %s;"
    
    # Copy the filter parameters and add pagination parameters
    query_params_main = query_params.copy()
    query_params_main.extend([page_size, offset])
    
    cursor.execute(main_query, tuple(query_params_main))
    movies = cursor.fetchall()

    cursor.close()
    conn.close()
    
    return jsonify({
        "page": page,
        "page_size": page_size,
        "total": total,
        "movies": movies
    })


@app.route("/genres", methods=["GET"])
def get_genres():
    """Fetch all available genres for filtering."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT genre_name FROM genres ORDER BY genre_name;")
    genres = [row["genre_name"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(genres)

@app.route("/search_directors", methods=["GET"])
def search_directors():
    """Search for directors based on user input."""
    search_query = request.args.get("q", "").strip().lower()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if not search_query:
        cursor.execute("SELECT director_name FROM directors ORDER BY director_name ASC LIMIT 10;")
    else:
        cursor.execute("""
            SELECT director_name FROM directors 
            WHERE LOWER(director_name) LIKE %s 
            ORDER BY director_name ASC 
            LIMIT 10;
        """, (f"%{search_query}%",))

    directors = [row["director_name"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(directors)
@app.route("/languages", methods=["GET"])
def get_languages():
    """Fetch all available languages."""
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT language_name FROM languages ORDER BY language_name;")
    languages = [row["language_name"] for row in cursor.fetchall()]


    cursor.close()
    conn.close()

    return jsonify(languages)
@app.route("/search_actors", methods=["GET"])
def search_actors():
    """Search for actors based on user input."""
    search_query = request.args.get("q", "").strip().lower()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    if not search_query:
        cursor.execute("SELECT actor_name FROM actors ORDER BY actor_name ASC LIMIT 10;")
    else:
        cursor.execute("""
            SELECT actor_name FROM actors 
            WHERE LOWER(actor_name) LIKE %s 
            ORDER BY actor_name ASC 
            LIMIT 10;
        """, (f"%{search_query}%",))

    actors = [row["actor_name"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(actors)
@app.route("/movie_details", methods=["GET"])
def movie_details():
    """Fetch details for a single movie by title."""
    title = request.args.get("title", "").strip()

    if not title:
        return jsonify({"error": "No title provided"}), 400

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = """
        SELECT 
            movies.title,
            IFNULL(GROUP_CONCAT(DISTINCT genres.genre_name SEPARATOR ', '), 'Unknown') AS genre,
            IFNULL(movies.avg_rating, 0) AS avg_rating,
            IFNULL(movies.release_date, 'Unknown') AS release_date,
            IFNULL(movies.poster_url, '') AS poster_url,
            IFNULL(GROUP_CONCAT(DISTINCT directors.director_name SEPARATOR ', '), 'Unknown') AS directors,
            IFNULL(GROUP_CONCAT(DISTINCT actors.actor_name SEPARATOR ', '), 'Unknown') AS actors,
            IFNULL(movies.runtime, 'Unknown') AS runtime,
            IFNULL(languages.language_name, 'Unknown') AS language,
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
        LEFT JOIN languages ON movies.language_id = languages.id
        WHERE movies.title LIKE %s
        GROUP BY movies.movieId
    """
    cursor.execute(sql_query, (f"%{title}%",))
    movie = cursor.fetchone()

    cursor.close()
    conn.close()

    if not movie:
        return jsonify({"error": "Movie not found"}), 404

    return jsonify(movie)
@app.route("/planner/lists/<int:list_id>/movies", methods=["POST"])
def add_movie_to_planner():
    """Add a movie to a planner list."""
    if "user_id" not in session:
        return jsonify({"error": "User not logged in"}), 401

    user_id = session["user_id"]
    list_id = request.json.get("list_id")
    movie_id = request.json.get("movieId")
    genre = request.json.get("genre")

    if not list_id or not movie_id or not genre:
        return jsonify({"error": "Missing required fields"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            "INSERT INTO planner_list_movies (list_id, movieId, genre) VALUES (%s, %s, %s)",
            (list_id, movie_id, genre)
        )
        conn.commit()
        return jsonify({"message": "Movie added successfully"}), 201
    except mysql.connector.Error as err:
        return jsonify({"error": str(err)}), 500
    finally:
        cursor.close()
        conn.close()

@app.route("/planner/lists/<int:list_id>/movies", methods=["GET"])
def get_movies_in_list(list_id):
    """Fetch all movies in a specific planner list."""
    if "user_id" not in session:
        return jsonify({"error": "User not logged in"}), 401

    user_id = session["user_id"]
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute(
        """
        SELECT movies.movieId, movies.title, movies.poster_url, planner_list_movies.genre
        FROM planner_list_movies
        JOIN movies ON planner_list_movies.movieId = movies.movieId
        JOIN planner_lists ON planner_list_movies.list_id = planner_lists.id
        WHERE planner_lists.id = %s AND planner_lists.user_id = %s
        """,
        (list_id, user_id),
    )
    movies = cursor.fetchall()

    cursor.close()
    conn.close()
    return jsonify(movies)

@app.route("/planner/lists/<int:list_id>/movies/<int:movie_id>", methods=["DELETE"])
def remove_movie_from_list(list_id, movie_id):
    """Remove a movie from a planner list."""
    if "user_id" not in session:
        return jsonify({"error": "User not logged in"}), 401

    user_id = session["user_id"]
    conn = get_db_connection()
    cursor = conn.cursor()

    # Ensure the list belongs to the user
    cursor.execute("SELECT id FROM planner_lists WHERE id = %s AND user_id = %s", (list_id, user_id))
    if not cursor.fetchone():
        return jsonify({"error": "List not found or not authorized"}), 403

    cursor.execute("DELETE FROM planner_list_movies WHERE list_id = %s AND movieId = %s", (list_id, movie_id))
    conn.commit()

    cursor.close()
    conn.close()
    return jsonify({"message": "Movie removed from planner list"}), 200
@app.route("/signup", methods=["POST"])
def signup():
    username = request.form.get("username")
    password = request.form.get("password")
    print("Signup attempted with:", username)  # Debug print

    if not username or not password:
        print("Missing username or password")
        return jsonify({"error": "Username and password required"}), 400

    # Hash the password using bcrypt
    import bcrypt
    hashed_password = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(
            "INSERT INTO users (username, password_hash) VALUES (%s, %s)",
            (username, hashed_password)
        )
        conn.commit()
        print("User inserted:", username)
    except mysql.connector.Error as err:
        print("Database error:", err)
        if err.errno == 1062:
            return jsonify({"error": "Username already exists"}), 409
        else:
            return jsonify({"error": str(err)}), 500
    finally:
        cursor.close()
        conn.close()
    
    return jsonify({"message": "Signup successful"}), 201

@app.route("/login", methods=["POST"])
def login():
    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()
    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT id, password_hash FROM users WHERE username = %s", (username,))
    user = cursor.fetchone()
    cursor.close()
    conn.close()
    if not user or not bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        return jsonify({"error": "Invalid username or password"}), 401
    session["user_id"] = user["id"]
    session["username"] = username
    return jsonify({"message": "Login successful", "username": username}), 200

@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    return jsonify({"message": "Logged out successfully"}), 200

@app.route("/check_session", methods=["GET"])
def check_session():
    if "user_id" in session:
        return jsonify({"logged_in": True, "username": session["username"]}), 200
    return jsonify({"logged_in": False}), 200
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000,debug=True)
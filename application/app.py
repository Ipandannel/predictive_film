import threading
from flask import Flask, jsonify, request, render_template, url_for,session,redirect
import mysql.connector
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64
import sys
import time
import bcrypt 
from flask_session import Session

app = Flask(__name__, static_folder='static', template_folder='templates')
app.config['SECRET_KEY'] = 'my_secret_key'
app.config['SESSION_TYPE'] = 'filesystem'
Session(app)
print("Secret Key:", app.config['SECRET_KEY'])
progress = {"low": 0, "high": 0}

def update_progress(key, start, end, duration):
    """
    Simulate progress update by increasing progress[key] from start to end over 'duration' seconds.
    """
    steps = end - start
    if steps <= 0:
        return
    delay = duration / steps
    for p in range(start + 1, end + 1):
        progress[key] = p
        time.sleep(delay)
def print_progress_bar(prefix, key):
    """
    Continuously print a progress bar in the terminal for the given key.
    The progress bar updates based on the value of progress[key] (0 to 100).
    """
    bar_length = 50  # Length of the progress bar in characters
    last_percent = -1
    while progress[key] < 100:
        percent = progress[key]
        if percent != last_percent:
            filled_length = int(round(bar_length * percent / 100))
            bar = '#' * filled_length + '-' * (bar_length - filled_length)
            sys.stdout.write(f'\r{prefix}: [{bar}] {percent}%')
            sys.stdout.flush()
            last_percent = percent
        time.sleep(0.1)
    # Print final state at 100%
    filled_length = bar_length
    bar = '#' * filled_length
    sys.stdout.write(f'\r{prefix}: [{bar}] 100%\n')
    sys.stdout.flush()

def get_db_connection():
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

def simulate_progress(prefix, duration=3):
    """
    Simulates a continuous progress bar in the terminal.
    prefix: A string label (e.g., "Low Rated Summary")
    duration: Total time (in seconds) over which to simulate progress.
    """
    bar_length = 50  # characters long progress bar
    for i in range(101):  # from 0% to 100%
        filled_length = int(round(bar_length * i / 100))
        bar = '#' * filled_length + '-' * (bar_length - filled_length)
        sys.stdout.write(f'\r{prefix}: [{bar}] {i}%')
        sys.stdout.flush()
        time.sleep(duration / 101)  # adjust duration to spread over the given time
    sys.stdout.write('\n')

def init_low_rated_summary():
    global progress
    progress["low"] = 0  # Reset progress for low-rated summary
    conn = get_db_connection()
    if conn is None:
        print("Low Rated Summary: Failed to connect to the database")
        return

    # Create summary table if not exists and check if data already exists
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS low_rated_summary (
            userId INT,
            low_rated_genre VARCHAR(255),
            other_genre VARCHAR(255),
            avg_other_rating FLOAT,
            rating_count INT,
            PRIMARY KEY (userId, low_rated_genre, other_genre)
        )
    """)
    conn.commit()
    cursor.execute("SELECT COUNT(*) as count FROM low_rated_summary")
    result = cursor.fetchone()
    if result and result["count"] > 0:
        print("Low Rated Summary already contains data. Skipping re-import.")
        cursor.close()
        conn.close()
        return

    # Start a thread to continuously print the progress bar
    low_progress_thread = threading.Thread(target=print_progress_bar, args=("Low Rated Summary", "low"), daemon=True)
    low_progress_thread.start()

    print("\nStarting low rated summary initialization...")
    update_progress("low", 0, 30, duration=2)  # Simulate progress 0% to 30%

    print("\nCreating and populating temporary table for low ratings...")
    create_and_populate_low_rated_temp_table(conn, "WHERE r.rating < 3.0")
    update_progress("low", 30, 50, duration=1)  # Simulate progress 30% to 50%

    print("\nTruncating (empty) result table for low ratings...")
    cursor.execute("TRUNCATE TABLE low_rated_summary")
    update_progress("low", 50, 80, duration=2)  # Simulate progress 50% to 80%

    print("\nCalculating and inserting summary data for low ratings...")
    cursor.execute("""
            INSERT IGNORE INTO low_rated_summary (userId, low_rated_genre, other_genre, avg_other_rating, rating_count)
            SELECT lr.userId, lr.genre AS low_rated_genre, g.genre_name AS other_genre, 
                   AVG(r.rating) AS avg_other_rating, COUNT(r.rating) AS rating_count
            FROM low_rated lr
            JOIN user_ratings r ON lr.userId = r.userId
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            WHERE r.movieId != lr.movieId               
            GROUP BY lr.userId, lr.genre, g.genre_name
            HAVING COUNT(r.rating) > 5
    """)
    update_progress("low", 80, 100, duration=1)  # Simulate progress 80% to 100%

    print(f"\nLow Rated Summary: {cursor.rowcount} rows inserted.")
    conn.commit()
    low_progress_thread.join()  # Wait for the progress bar thread to finish
    print("Low Rated Summary initialization complete.")
    cursor.close()
    conn.close()

def init_high_rated_summary():
    global progress
    progress["high"] = 0  # Reset progress for high-rated summary
    conn = get_db_connection()
    if conn is None:
        print("High Rated Summary: Failed to connect to the database")
        return

    # Create summary table if not exists and check if data already exists
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS high_rated_summary (
            userId INT,
            high_rated_genre VARCHAR(255),
            other_genre VARCHAR(255),
            avg_other_rating FLOAT,
            rating_count INT,
            PRIMARY KEY (userId, high_rated_genre, other_genre)
        )
    """)
    conn.commit()
    cursor.execute("SELECT COUNT(*) as count FROM high_rated_summary")
    result = cursor.fetchone()
    if result and result["count"] > 0:
        print("High Rated Summary already contains data. Skipping re-import.")
        cursor.close()
        conn.close()
        return

    # Start a thread to continuously print the progress bar for high ratings
    high_progress_thread = threading.Thread(target=print_progress_bar, args=("High Rated Summary", "high"), daemon=True)
    high_progress_thread.start()

    print("\nStarting high rated summary initialization...")
    update_progress("high", 0, 30, duration=2)  # Simulate progress 0% to 30%

    print("\nCreating and populating temporary table for high ratings...")
    create_and_populate_high_rated_temp_table(conn, "WHERE r.rating > 4.0")
    update_progress("high", 30, 50, duration=1)  # Simulate progress 30% to 50%

    print("\nTruncating (empty) result table for high ratings...")
    cursor.execute("TRUNCATE TABLE high_rated_summary")
    update_progress("high", 50, 80, duration=2)  # Simulate progress 50% to 80%

    print("\nCalculating and inserting summary data for high ratings...")
    cursor.execute("""
            INSERT IGNORE INTO high_rated_summary (userId, high_rated_genre, other_genre, avg_other_rating, rating_count)
            SELECT lr.userId, lr.genre AS high_rated_genre, g.genre_name AS other_genre, 
                   AVG(r.rating) AS avg_other_rating, COUNT(r.rating) AS rating_count
            FROM high_rated lr
            JOIN user_ratings r ON lr.userId = r.userId
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            WHERE r.movieId != lr.movieId
            GROUP BY lr.userId, lr.genre, g.genre_name
            HAVING COUNT(r.rating) > 5
    """)
    update_progress("high", 80, 100, duration=1)  # Simulate progress 80% to 100%

    print(f"\nHigh Rated Summary: {cursor.rowcount} rows inserted.")
    conn.commit()
    high_progress_thread.join()  # Wait for the progress bar thread to finish
    print("High Rated Summary initialization complete.")
    cursor.close()
    conn.close()


def create_and_populate_low_rated_temp_table(conn, condition_query, params=None):
    cursor = conn.cursor()
    try:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS low_rated (
                userId INT NOT NULL,
                movieId INT NOT NULL,
                rating FLOAT NOT NULL,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(50) NOT NULL,
                PRIMARY KEY (userId, movieId, genre)
            )
        """)

        cursor.execute("TRUNCATE TABLE low_rated")

        cursor.execute(f"""
            INSERT IGNORE INTO low_rated (userId, movieId, rating, title, genre)
            SELECT r.userId, r.movieId, r.rating, m.title, g.genre_name
            FROM user_ratings r
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            {condition_query}
        """, params or ())
        conn.commit()
        print("low_rated temporary table created and populated successfully.")
    except mysql.connector.Error as err:
        print(f"Failed to create or populate low_rated temp table: {err}")
        conn.rollback()
        raise
    finally:
        cursor.close()


def create_and_populate_high_rated_temp_table(conn, condition_query, params=None):
    cursor = conn.cursor()
    try:

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS high_rated (
                userId INT NOT NULL,
                movieId INT NOT NULL,
                rating FLOAT NOT NULL,
                title VARCHAR(255) NOT NULL,
                genre VARCHAR(50) NOT NULL,
                PRIMARY KEY (userId, movieId, genre)
            )
        """)

        cursor.execute("TRUNCATE TABLE high_rated")

        cursor.execute(f"""
            INSERT IGNORE INTO high_rated (userId, movieId, rating, title, genre)
            SELECT r.userId, r.movieId, r.rating, m.title, g.genre_name
            FROM user_ratings r
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            {condition_query}
        """, params or ())
        conn.commit()
        print("high_rated temporary table created and populated successfully.")
    except mysql.connector.Error as err:
        print(f"Failed to create or populate high_rated temp table: {err}")
        conn.rollback()
        raise
    finally:
        cursor.close()



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

    if query:
        base_query += " AND LOWER(movies.title) LIKE LOWER(%s)"
        query_params.append(f"%{query}%")

    if genres:
        base_query += """
            AND movies.movieId IN (
                SELECT mg.movieId FROM movie_genres mg 
                JOIN genres g ON mg.genreId = g.id 
                WHERE g.genre_name IN ({})
            )
        """.format(", ".join(["%s"] * len(genres)))
        query_params.extend(genres)

    if director:
        # Split the incoming string on commas and remove extra spaces
        directors_list = [d.strip() for d in director.split(",") if d.strip()]
        if directors_list:
            # Build OR conditions for each director
            director_conditions = " OR ".join(["d.director_name LIKE %s"] * len(directors_list))
            base_query += f"""
                AND movies.movieId IN (
                    SELECT md.movieId FROM movie_directors md 
                    JOIN directors d ON md.director_id = d.id 
                    WHERE {director_conditions}
                )
            """
            # Append a parameter for each director condition
            for d in directors_list:
                query_params.append(f"%{d}%")


    # Filter by actor with OR condition for multiple selections
    if actor:
        actors_list = [a.strip() for a in actor.split(",") if a.strip()]
        if actors_list:
            actor_conditions = " OR ".join(["a.actor_name LIKE %s"] * len(actors_list))
            base_query += f"""
                AND movies.movieId IN (
                    SELECT ma.movieId FROM movie_actors ma 
                    JOIN actors a ON ma.actor_id = a.id 
                    WHERE {actor_conditions}
                )
            """
            for a in actors_list:
                query_params.append(f"%{a}%")
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

    cursor.execute("SELECT DISTINCT genre_name FROM genres ORDER BY genre_name;")
    genres = [row["genre_name"] for row in cursor.fetchall()]

    cursor.close()
    conn.close()

    return jsonify(genres)




@app.route("/analyze/user_genre_rating_boxplot", methods=["GET"])
def user_genre_rating_boxplot():
    user_id = request.args.get("userId")
    if not user_id:
        return jsonify({"error": "userId is required"}), 400

    try:
        user_id = int(user_id)  
    except ValueError:
        return jsonify({"error": "userId must be an integer"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    try:
        cursor = conn.cursor(dictionary=True)

        cursor.execute("""
            SELECT r.userId, g.genre_name AS genre, r.rating
            FROM user_ratings r
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            WHERE r.userId = %s

        """, (user_id,))
        data = cursor.fetchall()

        if not data or len(data) == 0:
            return jsonify({"error": f"No data available for userId {user_id}"}), 200


        df = pd.DataFrame(data)
        plt.figure(figsize=(12, 6))
        sns.boxplot(data=df, x='genre', y='rating')
        plt.title(f'Rating Distribution for User {user_id} Across Genres')
        plt.xlabel('Genre')
        plt.ylabel('Rating')
        plt.xticks(rotation=45)


        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        image_base64 = base64.b64encode(buf.getvalue()).decode('utf-8')
        buf.close()
        plt.close()

        return jsonify({"image": image_base64})
    except mysql.connector.Error as err:
        return jsonify({"error": f"Query failed: {err}"}), 500
    except Exception as err:
        return jsonify({"error": f"Visualization failed: {err}"}), 500
    finally:
        cursor.close()
        conn.close()


@app.route("/analyze/filtered_low_ratings", methods=["GET"])
def filtered_low_ratings():
    user_id = request.args.get("userId")
    genre = request.args.get("genre")
    if not user_id or not genre:
        return jsonify({"error": "userId and genre are required"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT userId, low_rated_genre, other_genre, avg_other_rating, rating_count
            FROM low_rated_summary
            WHERE userId = %s AND low_rated_genre = %s AND avg_other_rating <= 3
            ORDER BY other_genre
        """, (user_id, genre))
        data = cursor.fetchall()

        if not data or len(data) == 0:
            return jsonify({"message": f"No data with avg_other_rating <= 3 for user {user_id} and genre {genre}"}), 200


        table_html = """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th>User ID</th>
                    <th>Low Rated Genre</th>
                    <th>Other Genre</th>
                    <th>Avg Other Rating</th>
                    <th>Rating Count</th>
                </tr>
            </thead>
            <tbody>
        """
        for row in data:
            table_html += f"""
                <tr>
                    <td>{row['userId']}</td>
                    <td>{row['low_rated_genre']}</td>
                    <td>{row['other_genre']}</td>
                    <td>{row['avg_other_rating']:.1f}</td>
                    <td>{row['rating_count']}</td>
                </tr>
            """
        table_html += """
            </tbody>
        </table>
        """

        return jsonify({"table_html": table_html})
    except mysql.connector.Error as err:
        return jsonify({"error": f"Query failed: {err}"}), 500
    except Exception as err:
        return jsonify({"error": f"Table generation failed: {err}"}), 500
    finally:
        cursor.close()
        conn.close()


@app.route("/analyze/filtered_high_ratings", methods=["GET"])
def filtered_high_ratings():
    user_id = request.args.get("userId")
    genre = request.args.get("genre")
    if not user_id or not genre:
        return jsonify({"error": "userId and genre are required"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT userId, high_rated_genre, other_genre, avg_other_rating, rating_count
            FROM high_rated_summary
            WHERE userId = %s AND high_rated_genre = %s AND avg_other_rating >= 4
            ORDER BY other_genre
        """, (user_id, genre))
        data = cursor.fetchall()

        if not data or len(data) == 0:
            return jsonify({"message": f"No data with avg_other_rating >= 4 for user {user_id} and genre {genre}"}), 200

        table_html = """
        <table border="1" style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th>User ID</th>
                    <th>High Rated Genre</th>
                    <th>Other Genre</th>
                    <th>Avg Other Rating</th>
                    <th>Rating Count</th>
                </tr>
            </thead>
            <tbody>
        """
        for row in data:
            table_html += f"""
                <tr>
                    <td>{row['userId']}</td>
                    <td>{row['high_rated_genre']}</td>
                    <td>{row['other_genre']}</td>
                    <td>{row['avg_other_rating']:.1f}</td>
                    <td>{row['rating_count']}</td>
                </tr>
            """
        table_html += """
            </tbody>
        </table>
        """

        return jsonify({"table_html": table_html})
    except mysql.connector.Error as err:
        return jsonify({"error": f"Query failed: {err}"}), 500
    except Exception as err:
        return jsonify({"error": f"Table generation failed: {err}"}), 500
    finally:
        cursor.close()
        conn.close()





@app.route("/record_genre", methods=["POST"])
def record_genre():
    data = request.get_json()
    genre = data.get("genre")

    if not genre:
        return jsonify({"success": False, "message": "Genre is required"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"success": False, "message": "Failed to connect to the database"}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id FROM genres WHERE genre_name = %s", (genre,))
        genre_exists = cursor.fetchone()

        if not genre_exists:
            return jsonify({"success": False, "message": "Genre not found in database"}), 404

        
        return jsonify({"success": True})
    finally:
        cursor.close()
        conn.close()





@app.route("/predict_rating", methods=["POST"])
def predict_rating():
    data = request.get_json()
    movie_id = data.get("movieId")
    title = data.get("title")
    genres = data.get("genres", [])

    if not movie_id or not title or not genres:
        return jsonify({"error": "Movie ID, title, and genres are required"}), 400

    try:
        movie_id = int(movie_id)
    except ValueError:
        return jsonify({"error": "Movie ID must be an integer"}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    try:
        cursor = conn.cursor()

        
        genre_ids = []
        for genre in genres:
            cursor.execute("SELECT id FROM genres WHERE genre_name = %s", (genre,))
            result = cursor.fetchone()
            if result:
                genre_ids.append(result[0])
            else:
                return jsonify({"error": f"Genre {genre} not found"}), 404

        if not genre_ids:
            return jsonify({"error": "No valid genres provided"}), 400

        
        genre_count = len(genre_ids)
        placeholders = ",".join(["%s"] * genre_count)
        query = f"""
        SELECT mg.movieId
        FROM movie_genres mg
        WHERE mg.genreId IN ({placeholders})
        GROUP BY mg.movieId
        HAVING COUNT(DISTINCT mg.genreId) = %s
        """
        params = genre_ids + [genre_count]
        cursor.execute(query, params)
        matching_movies = [row[0] for row in cursor.fetchall()]

        if not matching_movies:
            return jsonify({"error": "No movies found with the exact same genres"}), 404

        
        movie_placeholders = ",".join(["%s"] * len(matching_movies))
        rating_query = f"""
        SELECT AVG(r.rating)
        FROM user_ratings r
        WHERE r.movieId IN ({movie_placeholders})
        """
        cursor.execute(rating_query, matching_movies)
        avg_rating = cursor.fetchone()[0]

        if avg_rating is None:
            return jsonify({"error": "No ratings available for matching movies"}), 404

        
        cursor.execute(
            "INSERT INTO movies (movieId, title, avg_rating) VALUES (%s, %s, %s)",
            (movie_id, f"{title} (2025)", 0)
        )

        
        for genre in genres:
            cursor.execute(
                "INSERT INTO movie_genres (movieId, genreId) SELECT %s, id FROM genres WHERE genre_name = %s",
                (movie_id, genre)
            )

        conn.commit()
        
        
        

        return jsonify({"avg_rating": float(avg_rating)})
    except mysql.connector.Error as err:
        conn.rollback()
        return jsonify({"error": f"Database error: {err}"}), 500
    finally:
        cursor.close()
        conn.close()

@app.route("/genre-analysis")
def genre_analysis():
    print("Rendering genre_report.html template")
    return render_template("genre_report.html")

@app.route("/personality-analysis")
def personality_analysis():
    return render_template("personality_traits.html")
def print_progress(prefix, percent):
    bar_length = 50  # Length of the progress bar in characters
    filled_length = int(round(bar_length * percent / 100))
    bar = '#' * filled_length + '-' * (bar_length - filled_length)
    # '\r' returns the cursor to the beginning of the line.
    print(f'\r{prefix}: [{bar}] {percent}% Complete', end='', flush=True)

def background_low_init():
    print("Initializing low summary...")
    init_low_rated_summary()
    print("Initialization completed for low rated summary.")

def background_high_init():  
    print("Initializing high summary...")  
    init_high_rated_summary()
    print("Initialization completed for high rated summary.")
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
@app.route("/import_status", methods=["GET"])
def import_status():
    conn = get_db_connection()
    if conn is None:
        return jsonify({"complete": False})
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT COUNT(*) as count FROM movies")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    # Assuming that if there is at least one movie, the import is complete.
    complete = result and result["count"] > 0
    return jsonify({"complete": complete})

@app.route("/check_session", methods=["GET"])
def check_session():
    if "user_id" in session:
        return jsonify({"logged_in": True, "username": session["username"]}), 200
    return jsonify({"logged_in": False}), 200
threading.Thread(target=background_low_init, daemon=True).start()
threading.Thread(target=background_high_init, daemon=True).start()
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

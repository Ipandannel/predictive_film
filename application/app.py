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
            INSERT INTO low_rated_summary (userId, low_rated_genre, other_genre, avg_other_rating, rating_count)
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
            INSERT INTO high_rated_summary (userId, high_rated_genre, other_genre, avg_other_rating, rating_count)
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
    query = request.args.get("q", "").strip()
    genre = request.args.get("genre", "").strip()
    min_rating = request.args.get("min_rating", "").strip()
    max_rating = request.args.get("max_rating", "").strip()

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    sql_query = """
        SELECT m.movieId, m.title, 
               IFNULL(GROUP_CONCAT(DISTINCT g.genre_name SEPARATOR ', '), 'Unknown') AS genre,
               IFNULL(m.avg_rating, 0) AS avg_rating
        FROM movies m
        LEFT JOIN movie_genres mg ON m.movieId = mg.movieId
        LEFT JOIN genres g ON mg.genreId = g.id
        WHERE 1=1
    """
    query_params = []

    if query:
        sql_query += " AND LOWER(m.title) LIKE LOWER(%s)"
        query_params.append(f"%{query}%")

    if genre:
        sql_query += " AND g.genre_name = %s"
        query_params.append(genre)

    if min_rating:
        sql_query += " AND m.avg_rating >= %s"
        query_params.append(float(min_rating))
    if max_rating:
        sql_query += " AND m.avg_rating <= %s"
        query_params.append(float(max_rating))

    sql_query += " GROUP BY m.movieId LIMIT 10;"

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


threading.Thread(target=background_low_init, daemon=True).start()
threading.Thread(target=background_high_init, daemon=True).start()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

import threading

from flask import Flask, jsonify, request, render_template
import mysql.connector
import os
import pandas as pd
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import seaborn as sns
import io
import base64


app = Flask(__name__)

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



def init_low_rated_summary():
   
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    cursor = conn.cursor()

    print("initiate the temp table")
    create_and_populate_low_rated_temp_table(conn, "WHERE r.rating < 3.0")
    print("initiate the result table")

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
    cursor.execute("TRUNCATE TABLE low_rated_summary")
    print("calculate the result data and result it into the result table")

    cursor.execute("""
            INSERT INTO low_rated_summary (userId, low_rated_genre, other_genre, avg_other_rating, rating_count)
            SELECT lr.userId, lr.genre AS low_rated_genre, g.genre AS other_genre, 
                   AVG(r.rating) AS avg_other_rating, COUNT(r.rating) AS rating_count
            FROM low_rated lr
            JOIN ratings r ON lr.userId = r.userId
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            WHERE r.movieId != lr.movieId               
            GROUP BY lr.userId, lr.genre, g.genre
            HAVING COUNT(r.rating) > 5
    """)
    print(f"calculation finished:{cursor.rowcount}")
    conn.commit()
    cursor.close()
    conn.close()

def init_high_rated_summary():
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Failed to connect to the database"}), 500

    cursor = conn.cursor()

    print("initiate the temp table")
    create_and_populate_high_rated_temp_table(conn, "WHERE r.rating > 4.0")
    print("initiate the result table")

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
    cursor.execute("TRUNCATE TABLE high_rated_summary")
    print("calculate the result data and insert it into the result table")

    cursor.execute("""
            INSERT INTO high_rated_summary (userId, high_rated_genre, other_genre, avg_other_rating, rating_count)
            SELECT lr.userId, lr.genre AS high_rated_genre, g.genre AS other_genre, 
                   AVG(r.rating) AS avg_other_rating, COUNT(r.rating) AS rating_count
            FROM high_rated lr
            JOIN ratings r ON lr.userId = r.userId
            JOIN movies m ON r.movieId = m.movieId
            JOIN movie_genres mg ON m.movieId = mg.movieId
            JOIN genres g ON mg.genreId = g.id
            WHERE r.movieId != lr.movieId
            GROUP BY lr.userId, lr.genre, g.genre
            HAVING COUNT(r.rating) > 5
    """)
    print(f"calculation finished:{cursor.rowcount}")
    conn.commit()
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
            SELECT r.userId, r.movieId, r.rating, m.title, g.genre
            FROM ratings r
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
            SELECT r.userId, r.movieId, r.rating, m.title, g.genre
            FROM ratings r
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
               IFNULL(GROUP_CONCAT(DISTINCT g.genre SEPARATOR ', '), 'Unknown') AS genre,
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
        sql_query += " AND g.genre = %s"
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

    cursor.execute("SELECT DISTINCT genre FROM genres ORDER BY genre;")
    genres = [row["genre"] for row in cursor.fetchall()]

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
            SELECT r.userId, g.genre, r.rating
            FROM ratings r
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
        cursor.execute("SELECT id FROM genres WHERE genre = %s", (genre,))
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
            cursor.execute("SELECT id FROM genres WHERE genre = %s", (genre,))
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
        AND (SELECT COUNT(*) FROM movie_genres mg2 WHERE mg2.movieId = mg.movieId) = %s
        """
        params = genre_ids + [genre_count, genre_count]
        cursor.execute(query, params)
        matching_movies = [row[0] for row in cursor.fetchall()]

        if not matching_movies:
            return jsonify({"error": "No movies found with the exact same genres"}), 404

        
        movie_placeholders = ",".join(["%s"] * len(matching_movies))
        rating_query = f"""
        SELECT AVG(r.rating)
        FROM ratings r
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
                "INSERT INTO movie_genres (movieId, genreId) SELECT %s, id FROM genres WHERE genre = %s",
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


def background_low_init():
    print("Initializing summary...")
    init_low_rated_summary()
    print("Initialization completed for low rated summary.")

def background_high_init():    
    init_high_rated_summary()
    print("Initialization completed for high rated summary.")


threading.Thread(target=background_low_init, daemon=True).start()
threading.Thread(target=background_high_init, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

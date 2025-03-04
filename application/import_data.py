import mysql.connector
import os
import time
import csv

# Get database connection details from environment variables
DB_CONFIG = {
    "host": os.environ.get("DATABASE_HOST", "database"),
    "user": os.environ.get("DATABASE_USER", "root"),
    "password": os.environ.get("DATABASE_PASSWORD", "example"),
    "database": os.environ.get("DATABASE_NAME", "moviedb"),
}

# CSV file paths
MOVIES_CSV = "/dataset/movies.csv"
RATINGS_CSV = "/dataset/ratings.csv"
TAGS_CSV = "/dataset/tags.csv"

def connect_db(retries=10, delay=5):
    """Attempts to connect to MySQL, retrying if it fails, and ensures tables exist."""
    for attempt in range(retries):
        try:
            # Step 1: Connect to MySQL without specifying the database
            conn = mysql.connector.connect(
                host=DB_CONFIG["host"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                allow_local_infile=True
            )
            cursor = conn.cursor()

            # Step 2: Ensure the database exists
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
            cursor.close()
            conn.close()

            # Step 3: Now connect to the database
            conn = mysql.connector.connect(**DB_CONFIG, allow_local_infile=True)
            cursor = conn.cursor()

            # Step 4: Ensure required tables exist
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            # If no tables exist, MySQL is likely still initializing
            if not tables:
                print("⚠️ No tables found. Retrying...")
                raise mysql.connector.Error("Tables not initialized")

            cursor.close()
            return conn  # ✅ Connection successful!

        except mysql.connector.Error as err:
            print(f"Database connection failed: {err}. Retrying in {delay} seconds...")
            time.sleep(delay)

    raise RuntimeError("Database connection failed after multiple attempts.")
def is_data_imported(table_name):
    """Checks if data exists in the table."""
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0  # True if data exists, False otherwise

def import_movies():
    """Imports movie data including release_date and poster_url."""
    if is_data_imported("movies"):
        print("✅ Movies data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = """
    LOAD DATA INFILE '/dataset/movies.csv' 
    INTO TABLE movies
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (movieId, title, @genres, @release_date, @poster_url)
    SET 
        title = TRIM(title),
        release_date = CASE 
            WHEN @release_date = 'N/A' THEN NULL 
            ELSE STR_TO_DATE(@release_date, '%d-%b-%y') 
        END,
        poster_url = TRIM(@poster_url);
    """
    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Movies imported successfully with release_date and poster_url!")

def update_average_ratings():
    """Updates the avg_rating column in the movies table."""
    conn = connect_db()
    cursor = conn.cursor()

    update_query = """
    UPDATE movies
    SET avg_rating = (
        SELECT AVG(rating)
        FROM ratings
        WHERE ratings.movieId = movies.movieId
    );
    """

    cursor.execute(update_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Average ratings updated successfully!")
def import_genres():
    """Extract unique genres from movies.csv and insert them into the genres table."""
    print("📥 Extracting and importing genres...")

    conn = connect_db()
    cursor = conn.cursor()

    genre_set = set()
    movie_genres = []

    # Read genres directly from movies.csv
    with open("/dataset/movies.csv", "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)  # Skip header

        for row in reader:
            movieId = int(row[0])
            genres = row[2].split("|")  # Column index 2 contains genres

            for genre in genres:
                genre = genre.strip()
                if genre:
                    genre_set.add(genre)  # Store unique genres
                    movie_genres.append((movieId, genre))  # Store (movieId, genre)

    # Insert unique genres
    for genre in genre_set:
        cursor.execute("INSERT IGNORE INTO genres (genre) VALUES (%s)", (genre,))

    conn.commit()

    # Insert into movie_genres linking table
    for movieId, genre in movie_genres:
        cursor.execute(
            "INSERT IGNORE INTO movie_genres (movieId, genreId) SELECT %s, id FROM genres WHERE genre=%s",
            (movieId, genre)
        )

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Genres imported successfully!")

def import_ratings():
    """Imports ratings data using `LOAD DATA INFILE`."""
    if is_data_imported("ratings"):
        print("✅ Ratings data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = """
    LOAD DATA INFILE '/dataset/ratings.csv' 
    INTO TABLE ratings
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (userId, movieId, rating, timestamp);
    """
    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Ratings imported successfully!")

def import_tags():
    """Imports tags using `LOAD DATA INFILE`."""
    if is_data_imported("tags"):
        print("✅ Tags data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = """
    LOAD DATA INFILE '/dataset/tags.csv' 
    INTO TABLE tags
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"'
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (userId, movieId, tag, timestamp);
    """
    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tags imported successfully!")

def import_links():
    """Imports links using `LOAD DATA LOCAL INFILE`, allowing imports from any location."""
    if is_data_imported("links"):
        print("✅ Links data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = """
    LOAD DATA LOCAL INFILE '/dataset/links.csv'
    INTO TABLE links
    FIELDS TERMINATED BY ',' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (movieId, imdbId, @tmdbId)
    SET tmdbId = NULLIF(@tmdbId, '');
    """

    try:
        cursor.execute(query)
        conn.commit()
        print("✅ Links imported successfully!")
    except mysql.connector.Error as err:
        print(f"❌ Error importing links: {err}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    print("📥 Checking if data import is needed...")

    import_movies()
    import_genres()  # NEW FUNCTION to handle genres
    import_ratings()
    import_tags()
    import_links()
    update_average_ratings()

    print("🎉 Data import process completed!")

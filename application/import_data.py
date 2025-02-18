import mysql.connector
import os
import time

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

def connect_db(retries=5, delay=5):
    """Attempts to connect to the database, retrying if it fails."""
    for attempt in range(retries):
        try:
            conn = mysql.connector.connect(
                host=os.environ.get("DATABASE_HOST", "database"),
                user=os.environ.get("DATABASE_USER", "root"),
                password=os.environ.get("DATABASE_PASSWORD", "example"),
                database=os.environ.get("DATABASE_NAME", "moviedb"),
                allow_local_infile=True  
            )
            return conn
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
    """Imports movie data using MySQL's `LOAD DATA INFILE` for efficiency."""
    if is_data_imported("movies"):
        print("✅ Movies data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = f"""
    LOAD DATA INFILE '/dataset/movies.csv' 
    INTO TABLE movies
    FIELDS TERMINATED BY ',' 
    ENCLOSED BY '"' 
    LINES TERMINATED BY '\n'
    IGNORE 1 ROWS
    (movieId, title, genres);
    """
    
    cursor.execute(query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Movies imported successfully!")

def import_ratings():
    """Imports ratings data using `LOAD DATA INFILE`."""
    if is_data_imported("ratings"):
        print("✅ Ratings data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    query = f"""
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

    query = f"""
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

    query = f"""
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
    import_ratings()
    import_tags()
    import_links()

    print("🎉 Data import process completed!")

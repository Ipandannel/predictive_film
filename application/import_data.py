import mysql.connector
import os
import pandas as pd

# Get database connection details from environment variables
DB_CONFIG = {
    "host": os.environ.get("DATABASE_HOST", "database"),
    "user": os.environ.get("DATABASE_USER", "root"),
    "password": os.environ.get("DATABASE_PASSWORD", "example"),
    "database": os.environ.get("DATABASE_NAME", "moviedb"),
}

# File paths (ensure the dataset is in the same directory)
MOVIES_CSV = "ml-latest-small/movies.csv"
RATINGS_CSV = "ml-latest-small/ratings.csv"
TAGS_CSV = "ml-latest-small/tags.csv"

def connect_db():
    """Establishes a connection to the MySQL database."""
    return mysql.connector.connect(**DB_CONFIG)

def is_data_imported(table_name):
    """Checks if the specified table already contains data."""
    conn = connect_db()
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return count > 0  # Returns True if data exists, False otherwise

def import_movies():
    """Imports movie data into the MySQL database if not already imported."""
    if is_data_imported("movies"):
        print("✅ Movies data already exists. Skipping import.")
        return
    
    conn = connect_db()
    cursor = conn.cursor()
    
    df = pd.read_csv(MOVIES_CSV)
    
    for _, row in df.iterrows():
        cursor.execute(
            "INSERT IGNORE INTO movies (movieId, title, genres) VALUES (%s, %s, %s)",
            (row["movieId"], row["title"], row["genres"])
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Movies imported successfully!")

def import_ratings():
    """Imports user ratings into the MySQL database if not already imported."""
    if is_data_imported("ratings"):
        print("✅ Ratings data already exists. Skipping import.")
        return
    
    conn = connect_db()
    cursor = conn.cursor()

    df = pd.read_csv(RATINGS_CSV)
    
    # Ensure correct data types
    df["userId"] = df["userId"].astype(int)
    df["movieId"] = df["movieId"].astype(int)
    df["rating"] = df["rating"].astype(float)  # Convert rating to float
    df["timestamp"] = df["timestamp"].astype(int)  # Convert timestamp to int

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO ratings (userId, movieId, rating, timestamp) VALUES (%s, %s, %s, %s)",
            (int(row["userId"]), int(row["movieId"]), float(row["rating"]), int(row["timestamp"]))
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Ratings imported successfully!")


def import_tags():
    """Imports movie tags into the MySQL database if not already imported."""
    if is_data_imported("tags"):
        print("✅ Tags data already exists. Skipping import.")
        return
    
    conn = connect_db()
    cursor = conn.cursor()

    df = pd.read_csv(TAGS_CSV)
    
    # Ensure correct data types
    df["userId"] = df["userId"].astype(int)
    df["movieId"] = df["movieId"].astype(int)
    df["timestamp"] = df["timestamp"].astype(int)  # Convert timestamp to int

    for _, row in df.iterrows():
        cursor.execute(
            "INSERT INTO tags (userId, movieId, tag, timestamp) VALUES (%s, %s, %s, %s)",
            (int(row["userId"]), int(row["movieId"]), str(row["tag"]), int(row["timestamp"]))
        )
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tags imported successfully!")


if __name__ == "__main__":
    print("📥 Checking if data import is needed...")

    import_movies()
    import_ratings()
    import_tags()

    print("🎉 Data import process completed!")

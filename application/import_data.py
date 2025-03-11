import mysql.connector
import os
import time
import csv
from tqdm import tqdm 
import datetime
BATCH_SIZE=100
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
def batch_insert_movie_genres(cursor, movie_genres):
    """Batch inserts movie-genre relationships after genres exist."""
    cursor.executemany("""
        INSERT IGNORE INTO movie_genres (movieId, genreId)
        SELECT %s, id FROM genres WHERE genre_name=%s;
    """, movie_genres)

def import_movies():
    """Imports movies from movies.csv while normalizing related data, with batch inserts and a progress bar."""
    if is_data_imported("movies"):
        print("✅ Movies data already exists. Skipping import.")
        return
    conn = connect_db()
    cursor = conn.cursor()
    print("📥 Importing Movies...")
    # Read the CSV file
    with open(MOVIES_CSV, "r", encoding="utf-8") as file:
        reader = list(csv.reader(file))
        total_movies = len(reader) - 1  # Total rows (excluding header)
        reader = iter(reader)  # Convert back to iterator
        next(reader)  # Skip header

        movies_data = []
        movie_directors_data = []
        movie_actors_data = []
        movie_awards_data = []
        movie_ratings_data = []
        movie_genres_data = []
        genres_set = set()
        actors_set = set()
        directors_set = set()
        languages_set = set()
        primary_languages = {}  # {movieId: primary_language_name}

        for index, row in enumerate(tqdm(reader, total=total_movies, desc="Importing Movies", unit="movie")):
            movieId, title, genres, release_date, poster_url, imdb_rating, rt_score, director, actors, oscars, golden_globes, baftas, runtime, languages = row

            # ✅ Convert empty values to NULLs
            imdb_rating = float(imdb_rating) if imdb_rating != "N/A" else None
            rt_score = float(rt_score.replace("%", "")) if rt_score != "N/A" else None  # Convert percentage
            oscars = int(oscars) if oscars.isdigit() else 0
            golden_globes = int(golden_globes) if golden_globes.isdigit() else 0
            baftas = int(baftas) if baftas.isdigit() else 0

            # ✅ Convert release_date from DD-MMM-YY to YYYY-MM-DD
            if release_date and release_date != "N/A":
                try:
                    release_date = datetime.datetime.strptime(release_date, "%d-%b-%y").strftime("%Y-%m-%d")
                except ValueError:
                    release_date = None  # Set to NULL if invalid
            else:
                release_date = None

            # ✅ Detect and store the primary language
            detected_primary_language = None
            for language in languages.split(", "):  # Handle multiple languages
                language = language.strip()
                if language:
                    languages_set.add(language)  # ✅ Store unique languages
                    if not detected_primary_language:
                        detected_primary_language = language  # First detected is primary

            if detected_primary_language:
                primary_languages[movieId] = detected_primary_language

            # ✅ Collect Movies Data (without language_id for now)
            movies_data.append((movieId, title, release_date, poster_url, imdb_rating, runtime, None))

            # ✅ Collect Ratings Data
            movie_ratings_data.append((movieId, imdb_rating, rt_score))

            # ✅ Collect Awards Data
            movie_awards_data.append((movieId, oscars, golden_globes, baftas))

            # ✅ Collect Genres Data
            for genre in genres.split("|"):
                genre = genre.strip()
                if genre:
                    genres_set.add(genre)
                    movie_genres_data.append((movieId, genre))

            # ✅ Collect Directors Data (SPLIT MULTIPLE DIRECTORS)
            for dir_name in director.split(","):
                dir_name = dir_name.strip()
                if dir_name:
                    directors_set.add(dir_name)
                    movie_directors_data.append((movieId, dir_name))  # ✅ Store Director Relation

            # ✅ Collect Actors Data (SPLIT MULTIPLE ACTORS)
            for actor in actors.split(","):
                actor = actor.strip()
                if actor:
                    actors_set.add(actor)
                    movie_actors_data.append((movieId, actor))

            # ✅ Batch Insert Every `BATCH_SIZE` rows
            if index % BATCH_SIZE == 0:
                batch_insert(cursor, movies_data, movie_ratings_data, movie_awards_data)
                movies_data, movie_ratings_data, movie_awards_data = [], [], []

        # ✅ Final Batch Insert
        batch_insert(cursor, movies_data, movie_ratings_data, movie_awards_data)

        # ✅ Insert Languages First
        language_map = batch_insert_languages(cursor, languages_set)  # ✅ Get {language_name: language_id}
        conn.commit()  # 🔥 Ensure languages commit before next insert

        # ✅ Insert Genres
        batch_insert_genres(cursor, genres_set)
        conn.commit()  # 🔥 Ensure genres commit before next insert

        # ✅ Insert Directors
        batch_insert_directors(cursor, directors_set)
        conn.commit()  # 🔥 Ensure directors commit before next insert

        # ✅ Insert Actors
        batch_insert_actors(cursor, actors_set)
        conn.commit()  # 🔥 Ensure actors commit before next insert

        # ✅ Update `movies.language_id` using detected primary language
        update_movie_primary_languages(cursor, primary_languages, language_map)

        # ✅ Insert Director & Actor Relationships **AFTER** their tables exist
        batch_insert_movie_directors(cursor, movie_directors_data)
        batch_insert_movie_actors(cursor, movie_actors_data)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Movies, languages, directors, actors, and awards imported successfully!")

def batch_insert_movie_directors(cursor, movie_directors):
    """Batch inserts movie-director relationships after directors exist."""
    cursor.executemany("""
        INSERT IGNORE INTO movie_directors (movieId, director_id)
        SELECT %s, id FROM directors WHERE director_name=%s;
    """, movie_directors)


def batch_insert_movie_actors(cursor, movie_actors):
    """Batch inserts movie-actor relationships after actors exist."""
    cursor.executemany("""
        INSERT IGNORE INTO movie_actors (movieId, actor_id)
        SELECT %s, id FROM actors WHERE actor_name=%s;
    """, movie_actors)


def update_movie_primary_languages(cursor, primary_languages, language_map):
    """Updates the primary language_id in the movies table based on detected languages."""
    update_data = [
        (language_map[language], movieId)
        for movieId, language in primary_languages.items()
        if language in language_map
    ]

    if update_data:
        cursor.executemany("""
            UPDATE movies
            SET language_id = %s
            WHERE movieId = %s;
        """, update_data)
        print(f"✅ Updated primary language_id for {len(update_data)} movies.")


### ✅ **Batch Insert Helper Functions**
def batch_insert(cursor, movies, ratings, awards):
    """Performs batch insert for movies, ratings, and awards."""
    if movies:
        cursor.executemany("""
            INSERT INTO movies (movieId, title, release_date, poster_url, avg_rating, runtime, language_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE title=VALUES(title), release_date=VALUES(release_date), 
            poster_url=VALUES(poster_url), avg_rating=VALUES(avg_rating), runtime=VALUES(runtime), language_id=VALUES(language_id);
        """, movies)

    if ratings:
        cursor.executemany("""
            INSERT INTO ratings (movieId, imdb_rating, rotten_tomatoes)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE imdb_rating=VALUES(imdb_rating), rotten_tomatoes=VALUES(rotten_tomatoes);
        """, ratings)

    if awards:
        cursor.executemany("""
            INSERT INTO awards (movieId, oscars_won, golden_globes_won, baftas_won)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE oscars_won=VALUES(oscars_won), golden_globes_won=VALUES(golden_globes_won), baftas_won=VALUES(baftas_won);
        """, awards)


def batch_insert_genres(cursor, genres):
    """Inserts genres in batch."""
    cursor.executemany("INSERT IGNORE INTO genres (genre_name) VALUES (%s);", [(g,) for g in genres])



def batch_insert_languages(cursor, languages):
    """Inserts languages into the database while maintaining consistent language IDs."""
    
    # ✅ Insert unique languages
    cursor.executemany("INSERT IGNORE INTO languages (language_name) VALUES (%s);", [(l,) for l in languages])

    # ✅ Retrieve assigned language IDs
    cursor.execute("SELECT id, language_name FROM languages;")
    language_map = {row[1]: row[0] for row in cursor.fetchall()}  # {language_name: language_id}

    return language_map  # Return mapping for use in relationships


def batch_insert_directors(cursor, directors):
    """Inserts directors in batch."""
    cursor.executemany("INSERT IGNORE INTO directors (director_name) VALUES (%s);", [(d,) for d in directors])


def batch_insert_actors(cursor, actors):
    """Inserts actors in batch."""
    cursor.executemany("INSERT IGNORE INTO actors (actor_name) VALUES (%s);", [(a,) for a in actors])


def batch_insert_relationships(cursor, movie_languages, movie_directors, movie_actors):
    """Batch inserts relationships between movies and other entities."""
    
    # ✅ Insert Movie-Language Relations Correctly
    cursor.executemany("""
        INSERT IGNORE INTO movie_languages (movieId, language_id)
        SELECT %s, id FROM languages WHERE language_name=%s;
    """, movie_languages)

    # ✅ Now update `movies.language_id` directly for primary languages
    cursor.execute("""
        UPDATE movies m
        JOIN movie_languages ml ON m.movieId = ml.movieId
        SET m.language_id = ml.language_id
        WHERE m.language_id IS NULL;
    """)

    # ✅ Insert Movie-Director Relations
    cursor.executemany("""
        INSERT IGNORE INTO movie_directors (movieId, director_id)
        SELECT %s, id FROM directors WHERE director_name=%s;
    """, movie_directors)

    # ✅ Insert Movie-Actor Relations
    cursor.executemany("""
        INSERT IGNORE INTO movie_actors (movieId, actor_id)
        SELECT %s, id FROM actors WHERE actor_name=%s;
    """, movie_actors)

def update_average_ratings():
    """Updates the avg_rating column in the movies table based on user ratings."""
    conn = connect_db()
    cursor = conn.cursor()

    update_query = """
    UPDATE movies
    SET avg_rating = (
        SELECT AVG(user_ratings.rating)
        FROM user_ratings
        WHERE user_ratings.movieId = movies.movieId
    )
    WHERE EXISTS (
        SELECT 1 FROM user_ratings WHERE user_ratings.movieId = movies.movieId
    );
    """

    cursor.execute(update_query)
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Average ratings updated from user ratings successfully!")
def import_genres():
    """Extracts unique genres from movies.csv and inserts them into the genres table, then links movies to genres."""
    if is_data_imported("movie_genres"):  # 🔥 Check if relationships already exist
        print("✅ Movie-Genre relationships already exist. Skipping.")
        return

    print("📥 Extracting and importing genres...")

    conn = connect_db()
    cursor = conn.cursor()

    genre_set = set()
    movie_genres_data = []

    # Read genres directly from movies.csv
    with open(MOVIES_CSV, "r", encoding="utf-8") as file:
        reader = list(csv.reader(file))
        total_movies = len(reader) - 1  # Total rows (excluding header)
        reader = iter(reader)
        next(reader)  # Skip header

        for row in tqdm(reader, total=total_movies, desc="Importing Genres", unit="movie"):
            movieId = int(row[0])
            genres = row[2].split("|")  # Column index 2 contains genres

            for genre in genres:
                genre = genre.strip()
                if genre:
                    genre_set.add(genre)  # ✅ Store unique genres
                    movie_genres_data.append((movieId, genre))  # ✅ Store movie-genre pairs

    # ✅ Insert unique genres
    cursor.executemany("INSERT IGNORE INTO genres (genre_name) VALUES (%s);", [(g,) for g in genre_set])
    conn.commit()

    # ✅ Insert movie-genre relationships
    batch_insert_movie_genres(cursor, movie_genres_data)
    conn.commit()

    cursor.close()
    conn.close()
    print("✅ Genres and movie-genre relationships imported successfully!")


def import_ratings():
    """Imports ratings data using batch inserts and a progress bar."""
    if is_data_imported("user_ratings"):  # Check if user ratings exist
        print("✅ User Ratings data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    with open(RATINGS_CSV, "r", encoding="utf-8") as file:
        reader = list(csv.reader(file))
        total_ratings = len(reader) - 1  # Total rows (excluding header)
        reader = iter(reader)
        next(reader)  # Skip header

        ratings_data = []

        for row in tqdm(reader, total=total_ratings, desc="Importing Ratings", unit="rating"):
            userId, movieId, rating, timestamp = row
            ratings_data.append((userId, movieId, float(rating), int(timestamp)))

            # ✅ Batch insert every BATCH_SIZE rows
            if len(ratings_data) >= BATCH_SIZE:
                cursor.executemany("""
                    INSERT INTO user_ratings (userId, movieId, rating, timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE rating=VALUES(rating), timestamp=VALUES(timestamp);
                """, ratings_data)
                ratings_data = []

        # ✅ Final batch insert
        if ratings_data:
            cursor.executemany("""
                INSERT INTO user_ratings (userId, movieId, rating, timestamp)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE rating=VALUES(rating), timestamp=VALUES(timestamp);
            """, ratings_data)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ User Ratings imported successfully!")

def import_tags():
    """Imports tags using batch inserts and a progress bar."""
    if is_data_imported("tags"):
        print("✅ Tags data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    with open(TAGS_CSV, "r", encoding="utf-8") as file:
        reader = list(csv.reader(file))
        total_tags = len(reader) - 1  # Total rows (excluding header)
        reader = iter(reader)
        next(reader)  # Skip header

        tags_data = []

        for row in tqdm(reader, total=total_tags, desc="Importing Tags", unit="tag"):
            userId, movieId, tag, timestamp = row
            tags_data.append((userId, movieId, tag, int(timestamp)))

            # ✅ Batch insert every BATCH_SIZE rows
            if len(tags_data) >= BATCH_SIZE:
                cursor.executemany("""
                    INSERT INTO tags (userId, movieId, tag, timestamp)
                    VALUES (%s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE tag=VALUES(tag), timestamp=VALUES(timestamp);
                """, tags_data)
                tags_data = []

        # ✅ Final batch insert
        if tags_data:
            cursor.executemany("""
                INSERT INTO tags (userId, movieId, tag, timestamp)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE tag=VALUES(tag), timestamp=VALUES(timestamp);
            """, tags_data)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Tags imported successfully!")

def import_links():
    """Imports links using batch inserts and a progress bar."""
    if is_data_imported("links"):
        print("✅ Links data already exists. Skipping import.")
        return

    conn = connect_db()
    cursor = conn.cursor()

    with open("/dataset/links.csv", "r", encoding="utf-8") as file:
        reader = list(csv.reader(file))
        total_links = len(reader) - 1  # Total rows (excluding header)
        reader = iter(reader)
        next(reader)  # Skip header

        links_data = []

        for row in tqdm(reader, total=total_links, desc="Importing Links", unit="link"):
            movieId, imdbId, tmdbId = row
            tmdbId = tmdbId if tmdbId else None  # Convert empty tmdbId to NULL
            links_data.append((movieId, imdbId, tmdbId))

            # ✅ Batch insert every BATCH_SIZE rows
            if len(links_data) >= BATCH_SIZE:
                cursor.executemany("""
                    INSERT INTO links (movieId, imdbId, tmdbId)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE imdbId=VALUES(imdbId), tmdbId=VALUES(tmdbId);
                """, links_data)
                links_data = []

        # ✅ Final batch insert
        if links_data:
            cursor.executemany("""
                INSERT INTO links (movieId, imdbId, tmdbId)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE imdbId=VALUES(imdbId), tmdbId=VALUES(tmdbId);
            """, links_data)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Links imported successfully!")


if __name__ == "__main__":
    print("📥 Checking if data import is needed...")

    import_movies()
    import_genres()  # NEW FUNCTION to handle genres
    import_ratings()
    import_tags()
    import_links()
    update_average_ratings()

    print("🎉 Data import process completed!")

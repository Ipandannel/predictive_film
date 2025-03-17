CREATE DATABASE IF NOT EXISTS moviedb;
USE moviedb;
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL
);

-- Create Languages Table (Unique Language Entries)
CREATE TABLE IF NOT EXISTS languages (
    id INT AUTO_INCREMENT PRIMARY KEY,
    language_name VARCHAR(100) UNIQUE NOT NULL
);
-- Create Movies Table (WITH avg_rating)
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movieId INT UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    release_date DATE DEFAULT NULL,
    poster_url VARCHAR(500) DEFAULT NULL,
    avg_rating FLOAT DEFAULT NULL,  -- ✅ RESTORED avg_rating
    runtime VARCHAR(50) DEFAULT NULL,
    language_id INT DEFAULT NULL,
    FOREIGN KEY (language_id) REFERENCES languages(id) ON DELETE SET NULL
);


-- Create Genres Table (Stores Unique Genres)
CREATE TABLE IF NOT EXISTS genres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(50) UNIQUE NOT NULL
);

-- Create Movie-Genres Relationship Table (Many-to-Many)
CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INT NOT NULL,
    genreId INT NOT NULL,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genreId) REFERENCES genres(id) ON DELETE CASCADE
);

-- Create Directors Table
CREATE TABLE IF NOT EXISTS directors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    director_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create Movie-Directors Relationship Table (Many-to-Many)
CREATE TABLE IF NOT EXISTS movie_directors (
    movieId INT NOT NULL,
    director_id INT NOT NULL,
    PRIMARY KEY (movieId, director_id),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors(id) ON DELETE CASCADE
);

-- Create Actors Table
CREATE TABLE IF NOT EXISTS actors (
    id INT AUTO_INCREMENT PRIMARY KEY,
    actor_name VARCHAR(255) UNIQUE NOT NULL
);

-- Create Movie-Actors Relationship Table (Many-to-Many)
CREATE TABLE IF NOT EXISTS movie_actors (
    movieId INT NOT NULL,
    actor_id INT NOT NULL,
    PRIMARY KEY (movieId, actor_id),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (actor_id) REFERENCES actors(id) ON DELETE CASCADE
);

-- Create Ratings Table (Stores IMDb and Rotten Tomatoes Ratings)
CREATE TABLE IF NOT EXISTS ratings (
    movieId INT NOT NULL PRIMARY KEY,
    imdb_rating FLOAT DEFAULT NULL,
    rotten_tomatoes FLOAT DEFAULT NULL, -- Stored as a percentage (e.g., 85 for 85%)
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Create Awards Table (Stores Movie Awards)
CREATE TABLE IF NOT EXISTS awards (
    movieId INT NOT NULL PRIMARY KEY,
    oscars_won INT DEFAULT 0,
    golden_globes_won INT DEFAULT 0,
    baftas_won INT DEFAULT 0,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Create User Ratings Table (For Users Giving Ratings)
CREATE TABLE IF NOT EXISTS user_ratings (
    id INT AUTO_INCREMENT PRIMARY KEY,
    userId INT NOT NULL,
    movieId INT NOT NULL,
    rating FLOAT NOT NULL,
    timestamp BIGINT,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Create Tags Table
CREATE TABLE IF NOT EXISTS tags (
    id INT AUTO_INCREMENT PRIMARY KEY,
    userId INT NOT NULL,
    movieId INT NOT NULL,
    tag VARCHAR(255),
    timestamp BIGINT,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Create Links Table (External IDs)
CREATE TABLE IF NOT EXISTS links (
    movieId INT UNIQUE NOT NULL,
    imdbId INT NOT NULL,
    tmdbId INT NULL, 
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);
-- Create Festival Planner Lists Table
CREATE TABLE IF NOT EXISTS planner_lists (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    note VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);

-- Create Festival Planner List Movies Table
CREATE TABLE IF NOT EXISTS planner_list_movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    list_id INT NOT NULL,
    movieId INT NOT NULL,
    genre VARCHAR(50) NOT NULL,  -- Genre under which the movie is grouped in this list
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (list_id) REFERENCES planner_lists(id) ON DELETE CASCADE,
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    UNIQUE KEY unique_movie_in_list (list_id, movieId, genre)
);
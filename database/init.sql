CREATE DATABASE IF NOT EXISTS moviedb;
USE moviedb;

-- Create Movies Table (Genres Removed)
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movieId INT UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    release_date DATE DEFAULT NULL,  -- Added release date column
    poster_url VARCHAR(500) DEFAULT NULL,  -- Added poster URL column
    avg_rating FLOAT DEFAULT NULL  -- New column for average rating
);

-- Create Genres Table (Stores Unique Genres)
CREATE TABLE IF NOT EXISTS genres (
    id INT AUTO_INCREMENT PRIMARY KEY,
    genre VARCHAR(50) UNIQUE NOT NULL
);

-- Create Movie-Genres Relationship Table
CREATE TABLE IF NOT EXISTS movie_genres (
    movieId INT NOT NULL,
    genreId INT NOT NULL,
    PRIMARY KEY (movieId, genreId),
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE,
    FOREIGN KEY (genreId) REFERENCES genres(id) ON DELETE CASCADE
);

-- Create Ratings Table
CREATE TABLE IF NOT EXISTS ratings (
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

-- Create Links Table
CREATE TABLE IF NOT EXISTS links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movieId INT UNIQUE NOT NULL,
    imdbId INT NOT NULL,
    tmdbId INT NULL, 
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

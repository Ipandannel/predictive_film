CREATE DATABASE IF NOT EXISTS moviedb;
USE moviedb;
-- Create Movies Table
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movieId INT UNIQUE NOT NULL,
    title VARCHAR(255) NOT NULL,
    genres VARCHAR(255)
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
CREATE TABLE IF NOT EXISTS links (
    id INT AUTO_INCREMENT PRIMARY KEY,
    movieId INT UNIQUE NOT NULL,
    imdbId INT NOT NULL,
    tmdbId INT NULL, 
    FOREIGN KEY (movieId) REFERENCES movies(movieId) ON DELETE CASCADE
);

-- Create the movies table
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    release_year INT,
    genre VARCHAR(100)
);

-- Insert sample movie records
INSERT INTO movies (title, release_year, genre) VALUES
('Inception', 2010, 'Sci-Fi'),
('The Godfather', 1972, 'Crime'),
('Pulp Fiction', 1994, 'Crime');
ALTER USER 'root'@'%' IDENTIFIED WITH mysql_native_password BY 'example';
FLUSH PRIVILEGES;

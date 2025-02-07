from flask import Flask, jsonify
import mysql.connector
import os

app = Flask(__name__)

def get_db_connection():
    connection = mysql.connector.connect(
        host=os.environ.get("DATABASE_HOST", "database"),
        user=os.environ.get("DATABASE_USER", "root"),
        password=os.environ.get("DATABASE_PASSWORD", "example"),
        database=os.environ.get("DATABASE_NAME", "moviedb")
    )
    return connection

@app.route("/")
def index():
    return "Hello, this is your film festival app!"

@app.route("/movies")
def get_movies():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM movies;")
    movies = cursor.fetchall()
    cursor.close()
    conn.close()
    return jsonify(movies)

if __name__ == "__main__":
    # Make sure the Flask app listens on all interfaces so it can be reached from outside the container.
    app.run(host="0.0.0.0", port=5000)

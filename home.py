from flask import Flask, jsonify
import pymysql
import logging

app = Flask(__name__)

# 로깅 설정 추가
logging.basicConfig(level=logging.DEBUG)

def get_db_connection():
    try:
        conn = pymysql.connect(
            host="localhost",
            user="root",
            password="ckstn010324!",
            db="watchapedia",
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        logging.info("Database connection successful")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise

@app.route('/home/<int:user_id>', methods=['GET'])
def home(user_id):
    conn = None
    cursor = None
    try:
        logging.info(f"Processing request for user_id: {user_id}")
        conn = get_db_connection()
        cursor = conn.cursor()

        # 각 쿼리 실행 전에 로그 추가
        logging.info("Executing box office query")
        cursor.execute("""
            SELECT m.title AS movie_title, m.box_office AS is_box_office
            FROM movies m
            WHERE m.box_office = 1
            ORDER BY m.runtime DESC
            LIMIT 5
        """)
        box_office_movies = cursor.fetchall() or []

        logging.info("Executing recent reviews query")
        cursor.execute("""
            SELECT m.title AS movie_title, c.rating AS user_rating, c.comment AS user_comment
            FROM comments c
            JOIN movies m ON c.movie_id = m.movie_id
            WHERE c.user_id = %s
            ORDER BY c.comment_id DESC
            LIMIT 5
        """, (user_id,))
        recent_reviews = cursor.fetchall() or []

        logging.info("Executing favorite directors query")
        cursor.execute("""
            SELECT m.title AS movie_title, d.director_name AS director_name
            FROM favorite_directors fd
            JOIN directors d ON fd.director_id = d.director_id
            JOIN movies m ON d.director_id = m.director_id
            WHERE fd.user_id = %s
            LIMIT 5
        """, (user_id,))
        favorite_director_movies = cursor.fetchall() or []

        logging.info("Executing favorite actors query")
        cursor.execute("""
            SELECT m.title AS movie_title, a.actor_name AS actor_name
            FROM favorite_actors fa
            JOIN actors a ON fa.actor_id = a.actor_id
            JOIN movieactors ma ON a.actor_id = ma.actor_id
            JOIN movies m ON ma.movie_id = m.movie_id
            WHERE fa.user_id = %s
            LIMIT 5
        """, (user_id,))
        favorite_actor_movies = cursor.fetchall() or []

        response = {
            "box_office": box_office_movies,
            "recent_reviews": recent_reviews,
            "favorite_director_movies": favorite_director_movies,
            "favorite_actor_movies": favorite_actor_movies
        }

        logging.info("Request processed successfully")
        return jsonify(response)

    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed")

if __name__ == '__main__':
    app.run(debug=True, port=5000)
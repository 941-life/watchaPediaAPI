from flask import Flask, jsonify
import pymysql

app = Flask(__name__)

# Database connection function
def get_db_connection():
    return pymysql.connect(
        host="localhost",
        user="root",
        password="040320",
        database="watchapedia",
        cursorclass=pymysql.cursors.DictCursor
    )

@app.route('/movies', methods=['GET'])
def movies():
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 가장 별점이 높은 영화
        cursor.execute("""
            SELECT m.title AS movie_title, ROUND(AVG(c.rating), 1) AS average_rating
            FROM comments c
            JOIN movies m ON c.movie_id = m.movie_id
            GROUP BY m.title
            HAVING COUNT(c.comment_id) > 1 -- 최소 2개의 리뷰가 있는 영화만 반환
            ORDER BY average_rating DESC, m.title ASC
            LIMIT 5
        """)
        highest_rated_movies = cursor.fetchall()

        # 댓글이 가장 많은 영화
        cursor.execute("""
            SELECT m.title AS movie_title, COUNT(c.comment_id) AS total_comments
            FROM comments c
            JOIN movies m ON c.movie_id = m.movie_id
            GROUP BY m.title
            ORDER BY total_comments DESC, m.title ASC
            LIMIT 5
        """)
        most_commented_movies = cursor.fetchall()

        # 최신 영화 정렬
        cursor.execute("""
            SELECT m.title AS movie_title, m.runtime AS runtime, g.genre_name AS genre
            FROM movies m
            JOIN genres g ON m.genre_id = g.genre_id
            ORDER BY m.movie_id DESC
            LIMIT 5
        """)
        latest_movies = cursor.fetchall()

        # 응답 데이터 구성
        response = {
            "highest_rated_movies": highest_rated_movies if highest_rated_movies else "No data",
            "most_commented_movies": most_commented_movies if most_commented_movies else "No data",
            "latest_movies": latest_movies if latest_movies else "No data"
        }

    except Exception as e:
        response = {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)

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

@app.route('/home/<int:user_id>', methods=['GET'])
def home(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # 박스오피스 순위
        cursor.execute("""
            SELECT m.title AS movie_title, m.box_office AS is_box_office
            FROM movies m
            WHERE m.box_office = 1
            ORDER BY m.runtime DESC
            LIMIT 5
        """)
        box_office_movies = cursor.fetchall()

        # 최근에 평가한 영화
        cursor.execute("""
            SELECT m.title AS movie_title, c.rating AS user_rating, c.comment AS user_comment
            FROM comments c
            JOIN movies m ON c.movie_id = m.movie_id
            WHERE c.user_id = %s
            ORDER BY c.comment_id DESC
            LIMIT 5
        """, (user_id,))
        recent_reviews = cursor.fetchall()

        # 선호하는 감독 작품
        cursor.execute("""
            SELECT m.title AS movie_title, d.director_name AS director_name
            FROM favorite_directors fd
            JOIN directors d ON fd.director_id = d.director_id
            JOIN movies m ON d.director_id = m.director_id
            WHERE fd.user_id = %s
            LIMIT 5
        """, (user_id,))
        favorite_director_movies = cursor.fetchall()

        # 선호하는 배우 작품
        cursor.execute("""
            SELECT m.title AS movie_title, a.actor_name AS actor_name
            FROM favorite_actors fa
            JOIN actors a ON fa.actor_id = a.actor_id
            JOIN movieactors ma ON a.actor_id = ma.actor_id
            JOIN movies m ON ma.movie_id = m.movie_id
            WHERE fa.user_id = %s
            LIMIT 5
        """, (user_id,))
        favorite_actor_movies = cursor.fetchall()

        # 응답 데이터 구성
        response = {
            "box_office": box_office_movies if box_office_movies else "No data",
            "recent_reviews": recent_reviews if recent_reviews else "No data",
            "favorite_director_movies": favorite_director_movies if favorite_director_movies else "No data",
            "favorite_actor_movies": favorite_actor_movies if favorite_actor_movies else "No data"
        }

    except Exception as e:
        response = {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)

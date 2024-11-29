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

@app.route('/mypage/<int:user_id>', methods=['GET'])
def mypage(user_id):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        # Get average rating (취향분석 별점평균)
        cursor.execute("""
            SELECT ROUND(AVG(c.rating), 1) AS rating_average
            FROM comments c
            WHERE c.user_id = %s
        """, (user_id,))
        rating_average = cursor.fetchone()

        # Get favorite actors (좋아요한 배우)
        cursor.execute("""
            SELECT GROUP_CONCAT(DISTINCT a.actor_name SEPARATOR ', ') AS favorite_actors
            FROM favorite_actors fa
            JOIN actors a ON fa.actor_id = a.actor_id
            WHERE fa.user_id = %s
        """, (user_id,))
        favorite_actors = cursor.fetchone()

        # Get favorite directors (좋아요한 감독)
        cursor.execute("""
            SELECT GROUP_CONCAT(DISTINCT d.director_name SEPARATOR ', ') AS favorite_directors
            FROM favorite_directors fd
            JOIN directors d ON fd.director_id = d.director_id
            WHERE fd.user_id = %s
        """, (user_id,))
        favorite_directors = cursor.fetchone()

        # Response JSON
        response = {
            "user_id": user_id,
            "rating_average": rating_average["rating_average"] if rating_average and rating_average["rating_average"] else "No data",
            "favorite_actors": favorite_actors["favorite_actors"] if favorite_actors and favorite_actors["favorite_actors"] else "No data",
            "favorite_directors": favorite_directors["favorite_directors"] if favorite_directors and favorite_directors["favorite_directors"] else "No data"
        }

    except Exception as e:
        response = {"error": str(e)}
    finally:
        cursor.close()
        conn.close()

    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True)

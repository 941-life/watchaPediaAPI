from flask import Flask, jsonify
import pymysql
import logging

app = Flask(__name__)

logging.basicConfig(level=logging.DEBUG)

# 데이터베이스 연결 설정
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

# 1. 평가한 영화와 별점 조회
@app.route('/storage/<int:user_id>', methods=['GET'])
def storage(user_id):
    conn = None
    cursor = None
    try:
        logging.info(f"Processing request for user_id: {user_id}")
        conn = get_db_connection()
        cursor = conn.cursor()

        # SQL 쿼리 정의 (파라미터화된 쿼리 사용)
        queries = {
            "evaluation_movie": """
                SELECT m.title AS '평가한 영화', c.rating AS '내 별점'
                FROM movie_state s
                JOIN movies m ON m.movie_id = s.movie_id
                JOIN users u ON s.user_id = u.user_id
                JOIN comments c ON c.movie_id = m.movie_id AND c.user_id = u.user_id
                WHERE s.state = '평가한' AND u.user_id = %s;
            """,
            "want_movie": """
                SELECT m.title AS '보고싶어요한 영화'
                FROM movie_state s
                JOIN movies m ON m.movie_id = s.movie_id
                JOIN users u ON s.user_id = u.user_id
                WHERE s.state = '보고싶어요' AND u.user_id = %s;
            """,
            "watching_movie": """
                SELECT m.title AS '보는중인 영화'
                FROM movie_state s
                JOIN movies m ON m.movie_id = s.movie_id
                JOIN users u ON s.user_id = u.user_id
                WHERE s.state = '보는중' AND u.user_id = %s;
            """
        }

        # 각 쿼리를 실행하고 결과를 저장
        response_data = {}
        for key, query in queries.items():
            logging.info(f"Executing query for {key}")
            cursor.execute(query, (user_id,))
            response_data[key] = cursor.fetchall() or []

        logging.info("Request processed successfully")
        return jsonify(response_data)

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
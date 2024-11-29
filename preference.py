from flask import Flask, jsonify, request
import pymysql
import logging

app = Flask(__name__)

# 로깅 설정 (DEBUG 레벨로 설정하면 모든 로그 메시지가 출력됩니다.)
logging.basicConfig(level=logging.DEBUG)


# 데이터베이스 연결 설정
def get_db_connection():
    try:
        conn = pymysql.connect(
            host="localhost",
            user="root",
            password="0909",
            db="watchapedia",
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        logging.info("Database connection successful")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {str(e)}")
        raise


@app.route('/preference/<int:user_id>', methods=['GET'])  # URL 경로 수정
def preference(user_id):
    conn = None
    cursor = None
    try:
        logging.info(f"Processing request for user_id: {user_id}")
        conn = get_db_connection()
        cursor = conn.cursor()
        queries = {
            "특정 유저의 영화 평가 수": """
                SELECT 
                    COUNT(*) AS 평가수
                FROM comments
                WHERE user_id = %s;
            """,

            "특정 유저의 별점 빈도":
                """
                        SELECT 
                            c.rating AS 별점, 
                            COUNT(*) AS 빈도
                        FROM comments c
                        WHERE c.user_id = %s
                        GROUP BY c.rating
                        ORDER BY c.rating DESC;
                        """,

            "특정 유저의 별점 통계":
                """
                        SELECT 
                            ROUND(AVG(c.rating), 1) AS 별점평균, 
                            COUNT(c.rating) AS 별점개수,
                            (
                                SELECT c2.rating                
                                FROM comments c2
                                WHERE c2.user_id = %s
                                GROUP BY c2.rating
                                ORDER BY COUNT(*) DESC, c2.rating DESC
                                LIMIT 1
                            ) AS 많이준별점
                        FROM comments c
                        WHERE c.user_id = %s;
                        """,

            "유저 스타일 분석":

                """
                        SELECT 
                            CASE 
                                 WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 40 THEN '5점 뿌리는 ''부처님 급'' 아량의 소유자'
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 35 THEN '영화면 마냥 다 좋은 ''천사 급'' 착한 사람'
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 30 THEN '남 작품에 욕 잘 못하는 착한 품성의 ''돌고래 파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 25 THEN '별점에 다소 관대한 경향이 있는 ''다 주고파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 20 THEN '남들보다 별점을 조금 후하게 주는 ''인심파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 15 THEN '영화를 정말로 즐길 줄 아는 ''현명파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 10 THEN '편식 없이 골고루 보는 ''균형파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= 5 THEN '대중의 평가에 잘 휘둘리지 않는 ''지조파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -5 THEN '평가에 있어 주관이 뚜렷한 ''소나무파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -10 THEN '대체로 영화를 즐기지만 때론 혹평도 마다치 않는 ''이성파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -15 THEN '평가에 상대적으로 깐깐한 ''깐새우파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -20 THEN '작품을 남들보다 진지하고 비판적으로 보는 ''지성파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -25 THEN '작품을 대단히 냉정하게 평가하는 ''냉장고파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -30 THEN '웬만해서는 호평을 하지 않는 매서운 ''독수리파'''
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -35 THEN '별점을 대단히 짜게 주는 한줌의 ''소금'' 같은 분'
                                WHEN ROUND(((유저평균.별점 - 전체평균.별점) / 전체평균.별점) * 100, 2) >= -40 THEN '웬만해선 영화에 만족하지 않는 ''헝그리파'''
                                ELSE '세상 영화들에 불만이 많으신 ''개혁파'''
                            END AS 스타일
                        FROM (SELECT (SELECT ROUND(AVG(c.rating), 2) FROM comments c WHERE c.user_id = %s) 별점) 유저평균,
                             (SELECT (SELECT ROUND(AVG(c.rating), 2) FROM comments c) 별점) 전체평균;
                        """,

            "유저가 선호하는 태그":
                """
                        SELECT 
                            t.tag_name AS 태그이름
                        FROM movie_tags mt
                        JOIN comments c ON mt.movie_id = c.movie_id
                        JOIN tags t ON mt.tag_id = t.tag_id
                        WHERE c.user_id = %s
                        GROUP BY t.tag_name
                        ORDER BY (AVG(c.rating) * 20 * 0.4) + (COUNT(*) * 0.6) DESC
                        LIMIT 10;
                        """,

            "선호 배우 분석":
                """
                        SELECT 
                            a.actor_name AS 배우이름,
                            CONCAT(COUNT(*), '편') AS 본영화수,
                            CONCAT(
                                ROUND(
                                    ((COUNT(*) / (SELECT COUNT(*) FROM movieactors WHERE actor_id = MIN(ma.actor_id))) * 0.02) + 
                                    ((AVG(c.rating) * 20 * 0.98))), '점') AS 점수,
                            (SELECT m.title 
                             FROM movies m 
                             JOIN comments c2 ON m.movie_id = c2.movie_id 
                             JOIN movieactors ma2 ON ma2.movie_id = m.movie_id
                             WHERE ma2.actor_id = MIN(ma.actor_id) AND c2.user_id = %s 
                             ORDER BY c2.rating DESC 
                             LIMIT 1) AS 최고별점영화
                        FROM movieactors ma
                        JOIN comments c ON ma.movie_id = c.movie_id
                        JOIN actors a ON ma.actor_id = a.actor_id
                        WHERE c.user_id = %s
                        GROUP BY a.actor_name
                        HAVING 점수 >= 65
                        ORDER BY 점수 DESC
                        LIMIT 10;
                        """,

            "선호 감독 분석":
                """
                        SELECT 
                            d.director_name AS 감독이름,
                            CONCAT(COUNT(*), '편') AS 본영화수,
                            CONCAT(
                                ROUND(
                                    ((COUNT(*) / (SELECT COUNT(*) FROM movies WHERE director_id = MIN(m.director_id))) * 0.02) + 
                                    ((AVG(c.rating) * 20 * 0.98))), '점') AS 점수,
                            (SELECT m2.title 
                             FROM movies m2 
                             JOIN comments c2 ON m2.movie_id = c2.movie_id 
                             WHERE m2.director_id = MIN(m.director_id) AND c2.user_id = %s 
                             ORDER BY c2.rating DESC 
                             LIMIT 1) AS 최고별점영화
                        FROM movies m
                        JOIN comments c ON m.movie_id = c.movie_id
                        JOIN directors d ON m.director_id = d.director_id
                        WHERE c.user_id = %s
                        GROUP BY d.director_name
                        HAVING 점수 >= 65
                        ORDER BY 점수 DESC
                        LIMIT 10;
                        """,

            "선호 국가 분석":
                """
                        SELECT 
                            co.country_name AS 국가이름,
                            CONCAT(COUNT(*), '편') AS 본영화수,
                            CONCAT(
                                ROUND(((COUNT(*) / (SELECT COUNT(*) FROM comments WHERE user_id = %s)) *10) + (AVG(c.rating) * 20 * 0.9)), '점') AS 점수
                        FROM movies m
                        JOIN comments c ON m.movie_id = c.movie_id
                        JOIN countries co ON m.country_id = co.country_id
                        WHERE c.user_id = %s
                        GROUP BY co.country_name
                        ORDER BY 점수 DESC
                        LIMIT 6;
                        """,

            "선호 장르 분석":
                """
                        SELECT 
                            g.genre_name AS 장르이름,
                            CONCAT(COUNT(*), '편') AS 본영화수,
                            CONCAT(ROUND(((COUNT(*) / (SELECT COUNT(*) FROM comments WHERE user_id = %s)) * 10) + (AVG(c.rating) * 20 * 0.9)), '점') AS 점수
                        FROM movies m
                        JOIN comments c ON m.movie_id = c.movie_id
                        JOIN genres g ON m.genre_id = g.genre_id
                        WHERE c.user_id = %s
                        GROUP BY g.genre_name
                        ORDER BY 점수 DESC
                        LIMIT 10;
                        """,

            "총 영화 감상 시간 계산하기":
                """
                        SELECT 
                            CONCAT(FLOOR(SUM(m.runtime) / 60), ' 시간') AS 영화감상시간
                        FROM comments c
                        JOIN movies m ON c.movie_id = m.movie_id
                        WHERE c.user_id = %s;
                        """,

            "유저 순위 멘트 반환하기":
                """
                        SELECT 
                            CASE 
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 0.001 THEN '경지에 도달한 ''Film Master'''
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 0.005 THEN '국내에 몇 안되는 ''영화 Expert'''
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 0.01 THEN '상위 0.01퍼센트에 꼽히는 ''베테랑 영화인'''
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 0.03 THEN '영화 감독을 꿈꿀지 모를 ''영화 프로페셔널'''
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 0.1 THEN '상위 0.1퍼센트의 왓챠 보증 ''1등급 영화 내공인'''
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 1 THEN '상위 1퍼센트! 속옷은 갈아 입고 영화 보시죠'
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 3 THEN '영화 본 시간으로 상위 3퍼센트! 왓챠가 보증하는 영화 내공인'
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 5 THEN '상위 5퍼센트 진입! 공식적인 영화인입니다.'
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 30 THEN '상위 30퍼센트만큼 영화를 보셨어요. 그래도 상위권!'
                                WHEN PERCENT_RANK() OVER (ORDER BY FLOOR(SUM(m.runtime) / 60) DESC) * 100 <= 60 THEN '영화 본 시간으로 아직 평균에 못 미쳐요ᅲ'
                                ELSE '평가하는 거 나름 되게 재미있는데 어서 더 평가를...'
                            END AS 멘트
                        FROM comments c
                        JOIN movies m ON c.movie_id = m.movie_id
                        GROUP BY c.user_id
                        HAVING c.user_id = %s;
                        """

        }

        response_data = {}
        for key, query in queries.items():
            try:
                placeholder_count = query.count('%s')
                params = tuple([user_id] * placeholder_count)  # %s 개수만큼 user_id 반복
                logging.debug(f"Executing query for key: {key}")
                logging.debug(f"Query: {query}")
                logging.debug(f"Parameters: {params}")

                # 쿼리 실행
                cursor.execute(query, params)
                result = cursor.fetchall()

                # 결과 저장
                response_data[key] = result if result else {"message": "No data available"}
            except Exception as e:
                logging.error(f"Error in query '{key}': {e}")
                response_data[key] = {"error": str(e)}

        return jsonify(response_data), 200

    except Exception as e:
        logging.error(f"Error processing request: {str(e)}")
        return jsonify({"error": str(e)}), 500  # 500 Internal Server Error 반환
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        logging.info("Database connection closed")


if __name__ == '__main__':
    app.run(debug=True, port=5000)
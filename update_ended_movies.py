import asyncio
import time
from typing import List
from logging_config import get_logger
from database import get_db_pool, execute_query, execute_many

logger = get_logger(__name__)

MAX_DB_CONNECTIONS = 5
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

async def update_ended(cgv_movie_names: List[str], lotte_movie_names: List[str]) -> None:
    start_time = time.time()
    logger.info("상영 종료 영화 업데이트 시작")
    
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            movie_info = await conn.fetch("SELECT id, title FROM movie")
            
            cgv_ended_movies = [i['id'] for i in movie_info if i['title'] not in cgv_movie_names]
            lotte_ended_movies = [i['id'] for i in movie_info if i['title'] not in lotte_movie_names]

            theater_ids = {
                'CGV': await find_theater_id(conn, 'CGV'),
                '롯데시네마': await find_theater_id(conn, '롯데시네마')
            }

            for theater_name, ended_movies in [
                ('CGV', cgv_ended_movies),
                ('롯데시네마', lotte_ended_movies)
            ]:
                theater_id = theater_ids[theater_name]
                if theater_id and ended_movies:
                    if await find_movie_theater_info(conn, ended_movies, theater_id):
                        await delete_ended_movie_theater_info(conn, ended_movies, theater_id)
                        logger.info("영화 상영 정보 삭제 완료", extra={"theater": theater_name, "deleted_count": len(ended_movies)})
                else:
                    logger.warning("영화관 정보 없음 또는 종료된 영화 없음", extra={"theater": theater_name})

    except Exception as e:
        logger.exception("상영 종료 영화 업데이트 중 오류 발생")
    
    end_time = time.time()
    logger.info("상영 종료 영화 업데이트 완료", extra={"execution_time": f"{end_time - start_time:.2f}초"})

async def delete_ended_movie_theater_info(conn, movie_ids: List[int], theater_id: int) -> None:
    async with db_semaphore:
        query = """
        DELETE FROM movie_theaters
        WHERE movie_id = $1 AND theaters_id = $2
        """
        try:
            await conn.executemany(query, [(movie_id, theater_id) for movie_id in movie_ids])
        except Exception as e:
            logger.error("영화 상영 정보 삭제 중 오류 발생", extra={"error": str(e), "movie_ids": movie_ids, "theater_id": theater_id})
            raise

async def find_theater_id(conn, movie_theater_name: str) -> int:
    query = "SELECT id FROM theaters WHERE name = $1"
    result = await conn.fetchrow(query, movie_theater_name)
    return result['id'] if result else None

async def find_movie_theater_info(conn, movie_ids: List[int], theater_id: int) -> List[dict]:
    query = """
    SELECT id FROM movie_theaters
    WHERE movie_id = ANY($1) AND theaters_id = $2
    """
    return await conn.fetch(query, movie_ids, theater_id) # 실제 사용 시 영화 이름 리스트를 넣어주세요
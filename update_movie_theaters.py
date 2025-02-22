import asyncio
from typing import List, Dict, Any
from logging_config import get_logger
from database import get_db_pool, execute_query, execute_many, execute_transaction
from functools import lru_cache

logger = get_logger(__name__)

MAX_DB_CONNECTIONS = 5
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

@lru_cache(maxsize=None)
async def find_theater_id(conn, movie_theater_name: str) -> int:
    query = "SELECT id FROM theaters WHERE name = $1"
    result = await conn.fetchrow(query, movie_theater_name)
    return result['id'] if result else None

async def update_theaters_info(cgv_movie_names: List[str], lotte_movie_names: List[str]) -> None:
    logger.info("update_theaters_info 시작")
    try:
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            for theater_name, movies in [('CGV', cgv_movie_names), ('롯데시네마', lotte_movie_names)]:
                try:
                    await insert_unsaved_movie_theater_info(conn, movies, theater_name)
                    logger.info(f"{theater_name} 영화 정보 업데이트 완료")
                except Exception as e:
                    logger.error(f"{theater_name} 영화 정보 업데이트 중 오류 발생: {e}")
        logger.info("update_theaters_info 완료")
    except Exception as e:
        logger.error(f"update_theaters_info 중 오류 발생: {e}")

async def find_movie_info(conn, movie_names: List[str]) -> List[Dict[str, Any]]:
    query = "SELECT id FROM movie WHERE title = ANY($1)"
    return await conn.fetch(query, movie_names)

async def find_already_saved_movie_theater_info(conn, movie_ids: List[int], theater_id: int) -> List[Dict[str, Any]]:
    query = """
    SELECT theaters_id, movie_id 
    FROM movie_theaters 
    WHERE movie_id = ANY($1) AND theaters_id = $2
    """
    return await conn.fetch(query, movie_ids, theater_id)

async def save_movie_theaters_info(conn, movie_infos: List[Dict[str, Any]]) -> None:
    async with db_semaphore:
        query = """
        INSERT INTO movie_theaters (movie_id, theaters_id)
        VALUES ($1, $2)
        """
        await conn.executemany(query, [(info['movie_id'], info['theaters_id']) for info in movie_infos])

async def get_new_movie_infos(conn, movie_names: List[str], theater_id: int) -> List[Dict[str, Any]]:
    movie_info = await find_movie_info(conn, movie_names)
    movie_ids = [i['id'] for i in movie_info]
    
    existing_info = await find_already_saved_movie_theater_info(conn, movie_ids, theater_id)
    existing_movie_ids = [i['movie_id'] for i in existing_info]
    
    return [
        {'theaters_id': theater_id, 'movie_id': movie['id']}
        for movie in movie_info
        if movie['id'] not in existing_movie_ids
    ]

async def insert_unsaved_movie_theater_info(conn, movie_names: List[str], movie_theater_name: str) -> None:
    theater_id = await find_theater_id(conn, movie_theater_name)
    
    if theater_id is None:
        logger.error(f"영화관 '{movie_theater_name}'을(를) 찾을 수 없습니다.")
        return

    new_movie_infos = await get_new_movie_infos(conn, movie_names, theater_id)
    
    if new_movie_infos:
        await save_movie_theaters_info(conn, new_movie_infos)
        logger.info(f"{len(new_movie_infos)}개의 새로운 영화를 영화관 ID {theater_id}에 추가했습니다.")
    else:
        logger.info(f"영화관 ID {theater_id}에 추가할 새로운 영화가 없습니다.")
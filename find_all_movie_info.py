import asyncio
import time
from typing import List, Tuple
from cgv_movie_info import get_cgv_released_movie, get_cgv_releasing_movie
from lotte_movie_info import get_lotte_released_info, get_lotte_upcoming_info
from logging_config import get_logger

logger = get_logger(__name__)

def merge_movie_info(movie_info: List[str], movie_theater_info: List[str]) -> List[str]:
    return list(set(movie_info + movie_theater_info))

async def get_cgv_movies() -> Tuple[List[str], List[str]]:
    try:
        logger.info("CGV 영화 정보 가져오기 시작")
        start_time = time.time()
        released = await get_cgv_released_movie()
        releasing = await get_cgv_releasing_movie()
        end_time = time.time()
        logger.info(f"CGV 영화 정보 가져오기 완료 (소요 시간: {end_time - start_time:.2f}초)")
        return released, releasing
    except Exception as e:
        logger.error(f"CGV 영화 정보 가져오기 실패: {e}")
        return [], []

async def get_lotte_movies() -> Tuple[List[str], List[str]]:
    try:
        logger.info("롯데시네마 영화 정보 가져오기 시작")
        start_time = time.time()
        released = await get_lotte_released_info()
        upcoming = await get_lotte_upcoming_info()
        end_time = time.time()
        logger.info(f"롯데시네마 영화 정보 가져오기 완료 (소요 시간: {end_time - start_time:.2f}초)")
        return released, upcoming
    except Exception as e:
        logger.error(f"롯데시네마 영화 정보 가져오기 실패: {e}")
        return [], []

async def get_all_movie_info() -> Tuple[List[str], List[str]]:
    logger.info("모든 영화관 정보 가져오기 시작")
    start_time = time.time()
    
    cgv_task = asyncio.create_task(get_cgv_movies())
    lotte_task = asyncio.create_task(get_lotte_movies())
    
    tasks = [cgv_task, lotte_task]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    cgv_movie_names, lotte_movie_names = [], []
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"영화관 정보 가져오기 실패 (인덱스: {i}): {result}")
        else:
            if i == 0:
                cgv_movie_names = merge_movie_info(result[0], result[1])
            elif i == 1:
                lotte_movie_names = merge_movie_info(result[0], result[1])
    
    end_time = time.time()
    logger.info(f"모든 영화관 정보 가져오기 완료 (총 소요 시간: {end_time - start_time:.2f}초)")
    logger.info(f"CGV 영화 수: {len(cgv_movie_names)}, 롯데시네마 영화 수: {len(lotte_movie_names)}")
    
    return cgv_movie_names, lotte_movie_names
import asyncio
import aiohttp
from functools import wraps, lru_cache
import time
from typing import List, Dict, Any, Tuple
from logging_config import get_logger
from database import get_db_pool, execute_many, close_db_pool
from config import THE_MOVIE_DB_URL, THE_MOVIE_DB_TOKEN, MAX_CONCURRENT_REQUESTS

logger = get_logger(__name__)

semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

print(THE_MOVIE_DB_TOKEN)

def retry(max_tries=3, delay_seconds=1):
    def decorator_retry(func):
        @wraps(func)
        async def wrapper_retry(*args, **kwargs):
            tries = 0
            while tries < max_tries:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    tries += 1
                    if tries == max_tries:
                        raise e
                    logger.warning(f"{func.__name__} 실패. {delay_seconds}초 후 재시도... (시도 {tries}/{max_tries})")
                    await asyncio.sleep(delay_seconds)
        return wrapper_retry
    return decorator_retry

@lru_cache(maxsize=100)
async def get_existing_titles(conn, titles: Tuple[str]) -> List[str]:
    if not isinstance(titles, tuple):
        logger.error(f"Unexpected type for titles: {type(titles)}")
        logger.error(f"Content of titles: {titles[:1000]}")  # 처음 1000자만 로그
        raise TypeError("titles must be a tuple")

    query = "SELECT title FROM movie WHERE title = ANY($1)"
    result = await conn.fetch(query, list(titles))
    return [row['title'] for row in result]

async def update_upcoming_movie():
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + THE_MOVIE_DB_TOKEN
    }
    try:
        async with aiohttp.ClientSession() as session:
            upcoming_movies = await get_upcoming_movie_all_pages(session, THE_MOVIE_DB_URL, headers)
        
        logger.info(f"Retrieved {len(upcoming_movies)} upcoming movies")
        
        pool = await get_db_pool()
        async with pool.acquire() as conn:
            await insert_upcoming_movie_info(conn, upcoming_movies)
        logger.info(f"{len(upcoming_movies)}개의 영화 정보가 업데이트되었습니다.")
    except Exception as e:
        logger.exception(f"예상치 못한 오류 발생: {e}")
    finally:
        await close_db_pool()

@retry(max_tries=3, delay_seconds=2)
async def insert_upcoming_movie_info(conn, upcoming_movies: List[Any]):
    if not isinstance(upcoming_movies, list):
        logger.error(f"Unexpected type for upcoming_movies: {type(upcoming_movies)}")
        logger.error(f"Content of upcoming_movies: {upcoming_movies[:1000]}")  # 처음 1000자만 로그
        raise TypeError("upcoming_movies must be a list")

    if not upcoming_movies:
        logger.info("No upcoming movies to process")
        return

    if isinstance(upcoming_movies[0], dict):
        titles = tuple(movie['title'] for movie in upcoming_movies if isinstance(movie, dict) and 'title' in movie)
    elif isinstance(upcoming_movies[0], str):
        titles = tuple(upcoming_movies)
    else:
        logger.error(f"Unexpected data type in upcoming_movies: {type(upcoming_movies[0])}")
        return

    existing_titles = await get_existing_titles(conn, upcoming_movies)
    
    if isinstance(upcoming_movies[0], dict):
        new_movies = [movie for movie in upcoming_movies if movie['title'] not in existing_titles]
    else:
        new_movies = [movie for movie in upcoming_movies if movie not in existing_titles]
    
    if new_movies:
        await insert_new_movies(conn, new_movies)
    
    logger.info(f"{len(new_movies)}개의 새로운 영화가 추가되었습니다.")

async def get_existing_titles(conn, upcoming_movies: List[Any]) -> List[str]:
    if not upcoming_movies:
        return []

    if isinstance(upcoming_movies[0], dict):
        titles = [movie['title'] for movie in upcoming_movies if isinstance(movie, dict) and 'title' in movie]
    elif isinstance(upcoming_movies[0], str):
        titles = upcoming_movies
    else:
        logger.error(f"Unexpected data type in upcoming_movies: {type(upcoming_movies[0])}")
        return []

    query = "SELECT title FROM movie WHERE title = ANY($1)"
    result = await conn.fetch(query, titles)
    return [row['title'] for row in result]

async def insert_new_movies(conn, new_movies: List[Dict[str, Any]]):
    insert_query = """
    INSERT INTO movie (title, release_date, overview, poster_path, the_movie_db_id, is_theatrical_release)
    VALUES ($1, $2, $3, $4, $5, $6)
    """
    movie_data = [
        (m['title'], m['release_date'], m['overview'], m['poster_path'], m['the_movie_db_id'], m['is_theatrical_release'])
        for m in new_movies
    ]
    await conn.executemany(insert_query, movie_data)

@retry(max_tries=3, delay_seconds=2)
async def get_upcoming_movie_all_pages(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    current_page, total_pages, upcoming_movies = await get_first_page_upcoming_movie(session, url, headers)
    tasks = [get_page_data(session, f"{url}&page={page}", headers) for page in range(current_page + 1, total_pages + 1)]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            logger.error(f"Error fetching page: {result}")
        elif isinstance(result, dict) and 'results' in result:
            upcoming_movies += map_upcoming_movie_data(result['results'])
        else:
            logger.error(f"Unexpected result format: {result}")
    logger.info(f"Total upcoming movies: {len(upcoming_movies)}")
    return upcoming_movies

async def get_page_data(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> Dict[str, Any]:
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=30) as response:
                data = await response.json()
                if not isinstance(data, dict):
                    logger.error(f"Unexpected data format from API: {data}")
                return data
        except asyncio.TimeoutError:
            logger.error(f"Timeout while fetching data from {url}")
            return {}
        except Exception as e:
            logger.error(f"Error fetching data from {url}: {e}")
            return {}

async def get_first_page_upcoming_movie(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> Tuple[int, int, List[Dict[str, Any]]]:
    async with session.get(url, headers=headers) as response:
        data = await response.json()
    current_page = data['page']
    total_pages = data['total_pages']
    upcoming_movies = map_upcoming_movie_data(data['results'])
    return current_page, total_pages, upcoming_movies

def map_upcoming_movie_data(upcoming_movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    horrors = [x for x in upcoming_movies if 27 in x['genre_ids']]
    return [
        {
            "title": x['title'],
            "release_date": x['release_date'],
            "overview": x['overview'],
            "poster_path": x['poster_path'],
            "the_movie_db_id": x['id'],
            "is_theatrical_release": True
        }
        for x in horrors
    ]

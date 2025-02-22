import asyncio
import time
import traceback
from typing import List, Dict, Any, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException, TimeoutException
from logging_config import get_logger
from database import get_db_pool, execute_many
from config import UNOGS_URL, WAIT_TIME, EXPIRING_BUTTON_INDEX
from functools import lru_cache

logger = get_logger(__name__)

def setup_chrome_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    return webdriver.Chrome(options=chrome_options)

async def update_netflix_expiring_movie() -> Optional[bool]:
    try:
        expiring_movies = await find_netflix_expiring_movie()
        if not expiring_movies:
            logger.info("만료되는 영화가 없습니다.")
            return False

        pool = await get_db_pool()
        async with pool.acquire() as conn:
            netflix_horror_mv_en = await find_netflix_english_horror_movie(conn)
            expiring_horror_movies = find_expiring_horror_movies(expiring_movies, netflix_horror_mv_en)

            if not expiring_horror_movies:
                logger.info("만료되는 공포 영화가 없습니다.")
                return False

            the_movie_db_ids = [i['the_movie_db_id'] for i in expiring_horror_movies]
            the_movie_db_ids = await delete_if_netflix_horror_expiring_movie_exists(conn, the_movie_db_ids)

            if not the_movie_db_ids:
                logger.info("이미 저장된 만료되는 공포 영화가 있습니다.")
                return False

            result = await save_expiring_horror_movie(conn, expiring_horror_movies)
            if result:
                logger.info("만료되는 공포 영화를 저장했습니다.")
                return True
            else:
                logger.error("만료되는 공포 영화를 저장하지 못했습니다.")
                return False
    except Exception as e:
        logger.error(f"영화 업이트 중 오류 발생: {e}")
        logger.error(traceback.format_exc())
        return None

async def find_netflix_expiring_movie() -> List[Dict[str, str]]:
    driver = setup_chrome_driver()
    try:
        await asyncio.to_thread(driver.get, UNOGS_URL)
        await asyncio.to_thread(WebDriverWait(driver, WAIT_TIME).until, 
                                EC.presence_of_element_located((By.CSS_SELECTOR, "div.btn-group-vertical")))
        
        buttons = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, "div.btn-group-vertical button")
        await asyncio.to_thread(buttons[EXPIRING_BUTTON_INDEX].click)
        
        await asyncio.to_thread(WebDriverWait(driver, WAIT_TIME).until, 
                                EC.presence_of_element_located((By.CSS_SELECTOR, "table.table")))
        
        rows = await asyncio.to_thread(driver.find_elements, By.CSS_SELECTOR, "table.table tbody tr")
        
        expiring_movies = []
        for row in rows:
            columns = await asyncio.to_thread(row.find_elements, By.TAG_NAME, "td")
            if len(columns) >= 2:
                title = await asyncio.to_thread(columns[0].text)
                expired_date = await asyncio.to_thread(columns[1].text)
                expiring_movies.append({"title": title, "expired_date": expired_date})
        
        return expiring_movies
    except WebDriverException as e:
        logger.error(f"웹 드라이버 오류 발생: {e}")
        return []
    except TimeoutException:
        logger.error("페이지 로딩 시간 초과")
        return []
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        return []
    finally:
        await asyncio.to_thread(driver.quit)

@lru_cache(maxsize=1)
async def find_netflix_english_horror_movie(conn) -> List[Dict[str, Any]]:
    query = """
    SELECT title, EXTRACT(YEAR FROM release_date) AS release_year, the_movie_db_id
    FROM netflix_horror_en
    """
    return await conn.fetch(query)

def find_expiring_horror_movies(expiring_movies: List[Dict[str, str]], netflix_horror_mv_en: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    expiring_horror_movies = []
    for expiring_movie in expiring_movies:
        for horror_movie in netflix_horror_mv_en:
            if expiring_movie['title'].lower() in horror_movie['title'].lower():
                expiring_horror_movies.append({
                    'title': horror_movie['title'],
                    'expired_date': expiring_movie['expired_date'],
                    'the_movie_db_id': horror_movie['the_movie_db_id']
                })
                break
    return expiring_horror_movies

async def save_expiring_horror_movie(conn, expiring_horror_movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    MAX_DB_CONNECTIONS = 5
    db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)
    async with db_semaphore:
        query = """
        INSERT INTO netflix_horror_expiring (title, expired_date, the_movie_db_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (the_movie_db_id) DO UPDATE
        SET title = EXCLUDED.title,
            expired_date = EXCLUDED.expired_date
        RETURNING *
        """
        movie_data = [
            (movie['title'], movie['expired_date'], movie['the_movie_db_id'])
            for movie in expiring_horror_movies
        ]
        return await conn.executemany(query, movie_data)

async def delete_if_netflix_horror_expiring_movie_exists(conn, the_movie_db_ids: List[int]) -> List[int]:
    query = """
    SELECT the_movie_db_id
    FROM netflix_horror_expiring
    WHERE the_movie_db_id = ANY($1)
    """
    existing_ids = await conn.fetch(query, the_movie_db_ids)
    existing_ids = [row['the_movie_db_id'] for row in existing_ids]
    
    return [id for id in the_movie_db_ids if id not in existing_ids]
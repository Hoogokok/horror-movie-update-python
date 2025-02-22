import logging
import asyncio
from typing import List, Optional
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import WebDriverException
from tenacity import retry, stop_after_attempt, wait_exponential
from logging_config import get_logger
from config import CGV_RELEASED_URL, CGV_RELEASING_URL, CGV_RELEASED_CLICK_SELECTOR, TIMEOUT

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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
async def get_movie_titles(url: str, click_selector: Optional[str] = None) -> List[str]:
    driver = setup_chrome_driver()
    try:
        logger.info(f"영화 제목 가져오기 시작: {url}")
        await asyncio.to_thread(driver.get, url)
        
        if click_selector:
            target = await asyncio.to_thread(
                WebDriverWait(driver, TIMEOUT).until,
                EC.element_to_be_clickable((By.CSS_SELECTOR, click_selector))
            )
            await asyncio.to_thread(target.click)

        titles_web = await asyncio.to_thread(
            WebDriverWait(driver, TIMEOUT).until,
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'strong.title'))
        )
        return [title.text for title in titles_web]
    except WebDriverException as e:
        logger.error(f"웹 드라이버 오류 발생: {e}")
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        raise
    finally:
        await asyncio.to_thread(driver.quit)

async def get_cgv_released_movie() -> List[str]:
    return await get_movie_titles(CGV_RELEASED_URL, CGV_RELEASED_CLICK_SELECTOR)

async def get_cgv_releasing_movie() -> List[str]:
    return await get_movie_titles(CGV_RELEASING_URL)
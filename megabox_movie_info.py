import logging
import asyncio
import time
from typing import List
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException, StaleElementReferenceException
from tenacity import retry, stop_after_attempt, wait_exponential
from config import MEGABOX_RELEASED_URL, MEGABOX_UPCOMING_URL, TIMEOUT, MOVIE_SELECTOR, MORE_BUTTON_SELECTOR
from logging_config import get_logger

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
async def get_movie_titles(url: str, click_selector: str = None) -> List[str]:
    logger.info(f"영화 제목 가져오기 시작: {url}")
    start_time = time.time()
    driver = setup_chrome_driver()
    try:
        await asyncio.to_thread(driver.get, url)
        await asyncio.sleep(2)  # 페이지 로딩을 위한 대기

        if click_selector:
            previous_source = ""
            while True:
                try:
                    more_button = await asyncio.to_thread(
                        WebDriverWait(driver, TIMEOUT).until,
                        EC.element_to_be_clickable((By.CSS_SELECTOR, click_selector))
                    )
                    await asyncio.to_thread(more_button.click)
                    await asyncio.sleep(1)  # 클릭 후 로딩을 위한 대기
                    current_source = await asyncio.to_thread(lambda: driver.page_source)
                    if previous_source == current_source:
                        break
                    previous_source = current_source
                except TimeoutException:
                    break
                except StaleElementReferenceException:
                    logger.warning("StaleElementReferenceException 발생. 다시 시도합니다.")
                    continue

        titles_web = await asyncio.to_thread(
            WebDriverWait(driver, TIMEOUT).until,
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, MOVIE_SELECTOR))
        )
        end_time = time.time()
        logger.info(f"영화 제목 가져오기 완료 (소요 시간: {end_time - start_time:.2f}초)")
        return [title.text for title in titles_web if title.text]
    except WebDriverException as e:
        logger.error(f"웹 드라이버 오류 발생: {e}")
        raise
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        raise
    finally:
        await asyncio.to_thread(driver.quit)

async def get_megabox_released_info() -> List[str]:
    return await get_movie_titles(MEGABOX_RELEASED_URL, 'div.onair-condition > button')

async def get_megabox_upcoming_info() -> List[str]:
    return await get_movie_titles(MEGABOX_UPCOMING_URL)

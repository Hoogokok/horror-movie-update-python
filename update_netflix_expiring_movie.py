import os
import time
import logging

from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from supabase import create_client, Client

logging.basicConfig(level=logging.INFO)
load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')


def update_netflix_expiring_movie():
    client = create_client(supabase_url, supabase_key)
    netflix_horror_mv_en = find_netflix_english_horror_movie(client)
    try:
        logging.info("만료되는 영화를 가져옵니다.")
        expiring_movies = find_netflix_expiring_movie()
    except Exception as e:
        logging.info("만료되는 영화를 가져오지 못했습니다.")
        logging.error(e)
        return None

    expiring_horror_movies = find_expiring_horror_movie(expiring_movies, netflix_horror_mv_en)

    if not expiring_horror_movies:
        logging.info("만료되는 공포 영화가 없습니다.")
        return None

    the_movie_db_ids = [i['the_movie_db_id'] for i in expiring_horror_movies]
    the_movie_db_ids = delete_if_netflix_horror_expiring_movie_exists(client, the_movie_db_ids)

    if not the_movie_db_ids:
        logging.info("이미 저장된 만료되는 공포 영화가 있습니다.")
        return None

    result = save_expiring_horror_movie(client, expiring_horror_movies)
    if result:
        logging.info("만료되는 공포 영화를 저장했습니다.")
    else:
        logging.error("만료되는 공포 영화를 저장하지 못했습니다.")
        logging.error(result)


def find_netflix_expiring_movie():
    # 크롬 옵션 설정
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    # 웹 사이트 열기
    driver.get('https://unogs.com/countrydetail/?q=&cl=348,&pt=stats&st=&p=1&ao=and')
    # 페이지 로드를 기다림
    time.sleep(10)
    # button.btn.btn-primary.btn-sm[data-bind="click:showExpiring"]
    targets = WebDriverWait(driver, 1000).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'button.btn.btn-primary.btn-sm[data-bind="click:showExpiring"]')
        )
    )
    targets[28].click()
    titles_web = WebDriverWait(driver, 1000).until(
        # span[data-bind="html:title"]'
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'span[data-bind="html:title"]')
        )
    )
    expired_date_web = WebDriverWait(driver, 1000).until(
        # span[data-bind="html:titledate"]'
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'span[data-bind="html:titledate"]')
        )
    )
    release_years_web = WebDriverWait(driver, 1000).until(
        # span[data-bind="html:year"]
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'span[data-bind="html:year"]')
        )
    )
    expiring_movies = []
    for i in range(len(titles_web)):
        expiring_movies.append({
            "title": titles_web[i].text,
            "expired_date": expired_date_web[i].text.split(' ')[2],
            "release_year": release_years_web[i].text
        })
    driver.quit()
    return expiring_movies


def find_netflix_english_horror_movie(client: Client):
    data, _ = client.table('netflix_horror_en').select('title', "release_date", "the_movie_db_id").execute()
    return data[1]


def find_expiring_horror_movie(expiring_movies: list, netflix_horror_mv_en: list):
    # expriing_movies에 있는 영화 중 제목과 개봉연도로 netflix_horror_mv_en에 있는 영화를 찾음
    # netflix_horror_mv_en의 개봉연도의 형식은 yyyy-mm-dd이므로 yyyy만 비교
    expiring_horror_movies = []
    for i in expiring_movies:
        for j in netflix_horror_mv_en:
            if i['title'] == j['title'] and i['release_year'] == j['release_date'][:4]:
                # title expired_date the_movie_db_id
                expiring_horror_movies.append({
                    "title": i['title'],
                    "expired_date": i['expired_date'],
                    "the_movie_db_id": j['the_movie_db_id']
                })

    return expiring_horror_movies


def save_expiring_horror_movie(client: Client, expiring_horror_movies: list):
    data, _ = client.table('netflix_horror_expiring').insert(expiring_horror_movies).execute()
    return data


def delete_if_netflix_horror_expiring_movie_exists(client: Client, the_movie_db_ids: list):
    data, _ = client.table('netflix_horror_expiring').select('the_movie_db_id').execute()
    # 이미 저장된 영화는 the_movie_db_ids에서 제거
    for i in data[1]:
        if i['the_movie_db_id'] in the_movie_db_ids:
            the_movie_db_ids.remove(i['the_movie_db_id'])

    return the_movie_db_ids

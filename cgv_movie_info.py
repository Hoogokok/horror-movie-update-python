import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def get_cgv_released_movie():
    # Set up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    # Open the website
    driver.get('http://www.cgv.co.kr/movies/?lt=1&ft=1')
    # Wait for the page to load
    # #chk_nowshow
    target = WebDriverWait(driver, 100).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#contents > div.wrap-movie-chart > div.sect-sorting > div > label')
        )
    )
    target.click()
    time.sleep(10)
    # Wait for the page to load
    # class="btn-more-fontbold"
    target = WebDriverWait(driver, 100).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, '.btn-more-fontbold')
        )
    )
    target.click()
    time.sleep(10)
    wait = WebDriverWait(driver, 200)
    # contents > div.wrap-movie-chart > div.sect-movie-chart > ol.list-more > li:nth-child(9) > div.box-contents > a > strong
    titles_web = WebDriverWait(driver, 100).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'strong.title')
        )
    )
    titles = []
    for i in titles_web:
        titles.append(i.text)
    driver.quit()
    return titles


def get_cgv_releasing_movie():
    # Set up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    # Open the website
    driver.get('http://www.cgv.co.kr/movies/pre-movies.aspx')
    # Wait for the page to load
    wait = WebDriverWait(driver, 200)
    # contents > div.wrap-movie-chart > div.sect-movie-chart > ol.list-more > li:nth-child(9) > div.box-contents > a > strong
    titles_web = WebDriverWait(driver, 100).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'strong.title')
        )
    )
    titles = []
    for i in titles_web:
        titles.append(i.text)
    driver.quit()
    return titles

import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def get_megabox_released_info():
    # Set up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)
    # Open the website
    driver.get('https://www.megabox.co.kr/movie')
    # Wait for the page to load
    wait = WebDriverWait(driver, 200)
    # contents > div > div.movie-list-util.mt40 > div:nth-child(1) > div.onair-condition > button
    target = WebDriverWait(driver, 100).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'div.onair-condition > button')
        )
    )
    target.click()

    more_button = WebDriverWait(driver, 600).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#btnAddMovie')
        )
    )
    previous_source = driver.page_source
    while True:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            more_button.click()
            time.sleep(20)
            more_button = WebDriverWait(driver, 200).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#btnAddMovie')))
            current_source = driver.page_source
            if previous_source == current_source:
                break
            previous_source = current_source
        except:
            break

    titles_web = WebDriverWait(driver, 100).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'p.tit')
        )
    )

    titles = []
    for title in titles_web:
        titles.append(title.text)

    titles = list(filter(lambda x: x != '', titles))
    return titles


def get_megabox_upcoming_info():
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
    driver.get("https://www.megabox.co.kr/movie/comingsoon")

    more_button = WebDriverWait(driver, 600).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, '#btnAddMovie')
        )
    )
    previous_source = driver.page_source
    while True:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            more_button.click()
            time.sleep(20)
            more_button = WebDriverWait(driver, 200).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, '#btnAddMovie')))
            current_source = driver.page_source
            if previous_source == current_source:
                break
            previous_source = current_source
        except Exception as e:
            print(e)
            break

    titles_web = WebDriverWait(driver, 100).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'p.tit')
        )
    )

    titles = []
    for title in titles_web:
        titles.append(title.text)

    titles = list(filter(lambda x: x != '', titles))
    return titles

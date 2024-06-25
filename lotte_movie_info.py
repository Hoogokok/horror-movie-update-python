import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


def get_lotte_released_info():
    # Set up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.lottecinema.co.kr/NLCHS/Movie/List?flag=1")
    WebDriverWait(driver, 200)
    # contents > div > ul.movie_list.type2 > li:nth-child(31) > div.btm_info > strong
    titles_web = WebDriverWait(driver, 200).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'div.btm_info > strong')
        )
    )
    titles = []
    for i in titles_web:
        titles.append(i.text)
    driver.quit()
    return titles


def get_lotte_upcoming_info():
    # Set up the Chrome options
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(options=chrome_options)

    driver.get("https://www.lottecinema.co.kr/NLCHS/Movie/List?flag=5")
    WebDriverWait(driver, 200)
    # <button type="button" class="btn_txt_more"><span>펼쳐보기</span></button>
    more_button = WebDriverWait(driver, 200).until(
        EC.presence_of_element_located(
            (By.CSS_SELECTOR, 'button.btn_txt_more')
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
                    (By.CSS_SELECTOR, 'button.btn_txt_more')))
            current_source = driver.page_source
            if previous_source == current_source:
                break
            previous_source = current_source
        except Exception as e:
            print(e)
            break
    # contents > div > ul.movie_list.type2 > li:nth-child(31) > div.btm_info > strong
    titles_web = WebDriverWait(driver, 200).until(
        EC.presence_of_all_elements_located(
            (By.CSS_SELECTOR, 'strong.tit_info')
        )
    )
    titles = []
    for i in titles_web:
        titles.append(i.text)
    driver.quit()
    return titles

import os
from dotenv import load_dotenv
import urllib.parse

# 환경 변수 로드
load_dotenv()

# 상수 정의
BASE_URL = os.getenv('BASE_URL')
HORROR_GENRE_ID = os.getenv('HORROR_GENRE_ID')
THE_MOVIE_DB_TOKEN = os.getenv('THE_MOVIE_DB_TOKEN')
THE_MOVIE_DB_URL = os.getenv('THE_MOVIE_DB_URL')

# PostgreSQL 연결 정보
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = os.getenv('DB_PORT')
DB_CONFIG = {
    "host": DB_HOST,
    "port": int(DB_PORT),  # 포트를 정수로 변환
    "database": DB_NAME,
    "user": DB_USER,
    "password": urllib.parse.quote_plus(DB_PASSWORD)
}

DB_DSN = f"postgresql://{DB_USER}:{urllib.parse.quote_plus(DB_PASSWORD)}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# 영화 공급자 ID 매핑
PROVIDER_MAPPING = {
    8: 1,  # 넷플릭스
    337: 2,    # 디즈니
    356: 3,  # 웨이브
    96: 4,   # 네이버
    3: 5     # 구글 플레이
}

# API 요청 헤더
HEADERS = {
    "accept": "application/json",
    "Authorization": f"Bearer {THE_MOVIE_DB_TOKEN}"
}

# UNOGS 정보
UNOGS_URL = os.getenv('UNOGS_URL')
WAIT_TIME = os.getenv('WAIT_TIME')
EXPIRING_BUTTON_INDEX = os.getenv('EXPIRING_BUTTON_INDEX')

# 영화 정보 스크래핑 정보
CGV_RELEASED_URL = os.getenv('CGV_RELEASED_URL')
CGV_RELEASING_URL = os.getenv('CGV_RELEASING_URL')
CGV_RELEASED_CLICK_SELECTOR = os.getenv('CGV_RELEASED_CLICK_SELECTOR')

# 메가박스 정보
MEGABOX_RELEASED_URL = os.getenv('MEGABOX_RELEASED_URL')
MEGABOX_UPCOMING_URL = os.getenv('MEGABOX_UPCOMING_URL')
MOVIE_SELECTOR = 'p.tit'
MORE_BUTTON_SELECTOR = '#btnAddMovie'

# 롯데시네마 정보
LOTTE_RELEASED_URL = os.getenv('LOTTE_RELEASED_URL')
LOTTE_UPCOMING_URL = os.getenv('LOTTE_UPCOMING_URL')
RELEASED_SELECTOR = 'div.btm_info > strong'
UPCOMING_SELECTOR = 'strong.tit_info'

# 타임아웃
TIMEOUT = os.getenv('TIMEOUT')

MAX_CONCURRENT_REQUESTS = int(os.getenv('MAX_CONCURRENT_REQUESTS', '5'))

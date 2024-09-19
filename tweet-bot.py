from dotenv import load_dotenv
import os
import tweepy
import datetime
from supabase import create_client, Client
load_dotenv()
load_dotenv(override=True)

API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')
ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
ACCESS_TOKEN_SECRET = os.getenv('ACCESS_TOKEN_SECRET')
Bearer_token = os.getenv('BEARER_TOKEN')
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')


twClient = tweepy.Client(
    consumer_key=API_KEY,
    consumer_secret=API_SECRET,
    access_token=ACCESS_TOKEN,
    access_token_secret=ACCESS_TOKEN_SECRET,
    bearer_token=Bearer_token
)

def tweet_movie_info():
    client = create_client(supabase_url, supabase_key)
    print('영화 정보를 가져오는 중입니다...')
    # 영화 정보를 가져옴 (영화 제목, 개봉일) 현재 날짜 이후 개봉예정 영화만 가져옴
    today = format(datetime.datetime.now(), '%Y-%m-%d')
    movie_info = client.table('upcoming_movie').select('title', 'release_date', 'overview','poster_path').gt('release_date',today).execute().data

    if not movie_info:
        return None
    
    # 개봉일 기준으로 정렬
    movie_info.sort(key=lambda x: x['release_date'])
    print(f'영화 정보를 트윗했습니다.')
    # 트윗
    for movie in movie_info:
        title = movie['title']
        release_date = movie['release_date']
        tweet = f'[{title}] \n개봉일: {release_date}'
        # 트윗
        api.create_tweet(text="Hello, World!")
        print(f'{title} 영화 정보를 트윗했습니다.')


if __name__ == '__main__':
    tweet_movie_info()

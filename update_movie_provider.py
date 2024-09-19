import requests
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
the_movie_db_token = os.getenv('THE_MOVIE_DB_TOKEN')
the_movie_db_url = os.getenv('THE_MOVIE_DB_URL')


url = os.getenv('THE_MOVIE_DB_PROVIDER_URL')

headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {the_movie_db_token}"
}

def get_discover_movie_all_pages(url, headers):
    (current_page, total_pages, upcoming_movies) = get_first_page_discover_movie(url, headers)
    for page in range(current_page + 1, total_pages + 1):
        response = requests.get(url + f"&page={page}", headers=headers).json()
        upcoming_movies += map_discover_movie_data(response['results'])

    return upcoming_movies


def get_first_page_discover_movie(url, headers):
    response = requests.get(url, headers=headers).json()
    current_page = response['page']
    total_pages = response['total_pages']
    upcoming_movies = map_discover_movie_data(response['results'])
    return current_page, total_pages, upcoming_movies


def map_discover_movie_data(upcoming_movies):
    horrors = list(filter(lambda x: 27 in x['genre_ids'], upcoming_movies))
    return list(map(lambda x: {
        "title": x['title'],
        "release_date": x['release_date'],
        "overview": x['overview'],
        "poster_path": x['poster_path'],
        "the_movie_db_id": x['id']
    }, horrors))
    
def insert_movie_info(client: Client, upcoming_movies: list):
     response = client.table('movie').insert(upcoming_movies).execute()
     return response.data
 
def get_movie_ids(client: Client):
    response = client.table('movie').select('id').execute()
    return response.data

def insert_movie_proiviers(client: Client, movie_ids: list):
    response = client.table('movie_providers').insert(movie_ids).execute()
    return response.data

#337은 디즈니 8은 넷플릭스 356은 웨이브 96은 네이버 3은 구글 플레이 
#데이터베이스 넣을 때 넷플릭스는 1 디즈니는 2 웨이브는 3 네이버는 4 구글 플레이는 5
def insert_all_movie_provider():
 
    movies = get_discover_movie_all_pages(url, headers)
    client = create_client(supabase_url, supabase_key)
    movie_ids = insert_movie_info(client, movies)
    movie_ids = list(map(lambda x: {"movie_id": x['id'], "the_provider_id": 1}, movie_ids))   
    result = insert_movie_proiviers(client, movie_ids)

# 영화 공급자 ID 매핑
provider_mapping = {
    337: 1,  # 넷플릭스
    8: 2,    # 디즈니
    356: 3,  # 웨이브
    96: 4,   # 네이버
    3: 5     # 구글 플레이
}

def get_movies_for_provider(provider_id):
    url = f"{the_movie_db_url}&with_watch_providers={provider_id}"
    movies = get_discover_movie_all_pages(url, headers)
    return movies

def insert_movies_with_provider(client: Client, movies: list, provider_id: int):
    mapped_provider_id = provider_mapping.get(provider_id)
    if not mapped_provider_id:
        print(f"알 수 없는 공급자 ID: {provider_id}")
        return
    
    for movie in movies:
        # 영화 정보 삽입 또는 업데이트
        movie_data, _ = client.table('movie').upsert({
            "the_movie_db_id": movie['the_movie_db_id'],
            "title": movie['title'],
            "release_date": movie['release_date'],
            "overview": movie['overview'],
            "poster_path": movie['poster_path']
        }).execute()
        
        # 영화 공급자 정보 삽입
        provider_data, _ = client.table('movie_providers').upsert({
            "movie_id": movie['the_movie_db_id'],
            "provider_id": mapped_provider_id
        }).execute()
    
    print(f"공급자 ID {provider_id} (매핑된 ID: {mapped_provider_id})의 영화 {len(movies)}개가 처리되었습니다.")

def update_all_providers():
    client = create_client(supabase_url, supabase_key)
    for provider_id in provider_mapping.keys():
        movies = get_movies_for_provider(provider_id)
        insert_movies_with_provider(client, movies, provider_id)


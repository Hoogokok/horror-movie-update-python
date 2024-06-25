import requests
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
the_movie_db_token = os.getenv('THE_MOVIE_DB_TOKEN')
the_movie_db_url = os.getenv('THE_MOVIE_DB_URL')


def update_upcoming_movie():
    headers = {
        "accept": "application/json",
        "Authorization": "Bearer " + the_movie_db_token
    }
    client = create_client(supabase_url, supabase_key)
    upcoming_movies = get_upcoming_movie_all_pages(the_movie_db_url, headers)
    insert_upcoming_movie_info(client, upcoming_movies)


def insert_upcoming_movie_info(client: Client, upcoming_movies: list):
    # 이미 저장된 영화인지 확인
    data = get_upcoming(client, upcoming_movies)
    # 저장되지 않은 영화만 저장
    titles = [i['title'] for i in data[1]]
    upcoming_movies = list(filter(lambda x: x['title'] not in titles, upcoming_movies))
    if upcoming_movies:
        data, _ = client.table('upcoming_movie').insert(upcoming_movies).execute()
        return data
    return None


def get_upcoming(client, upcoming_movies):
    data, _ = client.table('upcoming_movie').select('title').in_('title',
                                                                     [i['title'] for i in upcoming_movies]).execute()
    return data


def get_upcoming_movie_all_pages(url, headers):
    (current_page, total_pages, upcoming_movies) = get_first_page_upcoming_movie(url, headers)
    for page in range(current_page + 1, total_pages + 1):
        response = requests.get(url + f"&page={page}", headers=headers).json()
        upcoming_movies += map_upcoming_movie_data(response['results'])

    return upcoming_movies


def get_first_page_upcoming_movie(url, headers):
    response = requests.get(url, headers=headers).json()
    current_page = response['page']
    total_pages = response['total_pages']
    upcoming_movies = map_upcoming_movie_data(response['results'])
    return current_page, total_pages, upcoming_movies


def map_upcoming_movie_data(upcoming_movies):
    horrors = list(filter(lambda x: 27 in x['genre_ids'], upcoming_movies))
    return list(map(lambda x: {
        "title": x['title'],
        "release_date": x['release_date'],
        "overview": x['overview'],
        "poster_path": x['poster_path'],
    }, horrors))

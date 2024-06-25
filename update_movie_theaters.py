from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')


def update_theaters_info(cgv_movie_names, megabox_movie_names, lotte_movie_names):
    client = create_client(supabase_url, supabase_key)
    # 스크래핑한 영화 정보를 데이터베이스에 저장
    insert_unsaved_movie_theater_info(client, cgv_movie_names, 'CGV')
    insert_unsaved_movie_theater_info(client, megabox_movie_names, '메가박스')
    insert_unsaved_movie_theater_info(client, lotte_movie_names, '롯데시네마')


def find_movie_info(client: Client, movie_names: list):
    data, _ = client.table('upcoming_movie').select("pk").in_('title', movie_names).execute()
    return data[1]


def find_already_save_movie_theater_info(client: Client, movie_ids: list, theater_id: int):
    data, _ = client.table('movie_theaters').select("theaters_id", 'movie_id').in_('movie_id', movie_ids).eq(
        'theaters_id', theater_id).execute()
    return data[1]


def find_theater_id(client: Client, movie_theater_name: str):
    data, _ = client.table('theaters').select('id').eq('name', movie_theater_name).execute()
    return data[1][0]['id']


def save_movie_theaters_info(client: Client, movie_info: dict):
    data, _ = client.table('movie_theaters').insert([movie_info]).execute()
    return data[1]


def merge_movie_info(movie_info: list, movie_theater_info: list):
    return movie_info + movie_theater_info


def insert_unsaved_movie_theater_info(client: Client, movie_names: list, movie_theater_name: str):
    movie_info = find_movie_info(client, movie_names)
    movie_ids = [i['pk'] for i in movie_info]
    theater_id = find_theater_id(client, movie_theater_name)
    movie_theater_info = find_already_save_movie_theater_info(client, movie_ids, theater_id)
    movie_theater_info = [i['movie_id'] for i in movie_theater_info]
    for i in movie_info:
        if i['pk'] not in movie_theater_info:
            i['theaters_id'] = theater_id
            # pk를 movie_id로 바꿔야함
            i['movie_id'] = i.pop('pk')
            save_movie_theaters_info(client, i)

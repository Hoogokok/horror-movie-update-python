from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()
supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')


def update_ended(cgv_movie_names, megabox_movie_names, lotte_movie_names):
    client = create_client(supabase_url, supabase_key)

    # 데이터베이스에서 영화정보를 가져옴
    movie_info = client.table('upcoming_movie').select('pk', 'title').execute().data
    # 저장된 영화정보와 스크래핑한 영화정보를 비교하여 영화관에서 상영이 끝난 영화를 찾음
    cgv_ended_movies = []
    megabox_ended_movies = []
    lotte_ended_movies = []
    for i in movie_info:
        if i['title'] not in cgv_movie_names:
            cgv_ended_movies.append(i['pk'])

        if i['title'] not in megabox_movie_names:
            megabox_ended_movies.append(i['pk'])

        if i['title'] not in lotte_movie_names:
            lotte_ended_movies.append(i['pk'])

    # 영화관 아이디를 찾는다.
    cgv_theater_id = find_theater_id(client, 'CGV')
    megabox_theater_id = find_theater_id(client, '메가박스')
    lotte_theater_id = find_theater_id(client, '롯데시네마')

    # 상영관 정보가 저장되어 있는지 확인
    cgv_movie_theater_info = find_movie_theater_info(client, cgv_ended_movies, cgv_theater_id)
    megabox_movie_theater_info = find_movie_theater_info(client, megabox_ended_movies, megabox_theater_id)
    lotte_movie_theater_info = find_movie_theater_info(client, lotte_ended_movies, lotte_theater_id)

    # 상영관 정보가 저장되어 있으면 삭제
    if cgv_movie_theater_info:
        delete_ended_movie_theater_info(client, cgv_ended_movies, cgv_theater_id)
    if megabox_movie_theater_info:
        delete_ended_movie_theater_info(client, megabox_ended_movies, megabox_theater_id)
    if lotte_movie_theater_info:
        delete_ended_movie_theater_info(client, lotte_ended_movies, lotte_theater_id)


def delete_ended_movie_theater_info(client: Client, movie_ids: list, theater_id: int):
    data, _ = (client.table('movie_theaters').delete().in_('movie_id', movie_ids)
                   .eq('theaters_id', theater_id).execute())
    return data


def find_theater_id(client: Client, movie_theater_name: str):
    data, _ = client.table('theaters').select('id').eq('name', movie_theater_name).execute()
    return data[1][0]['id']


def find_movie_theater_info(client: Client, movie_ids: list, theater_id: int):
    return (client.table('movie_theaters').select('id').in_('movie_id', movie_ids)
            .eq('theaters_id', theater_id)
            .execute()
            .data)

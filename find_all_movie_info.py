from cgv_movie_info import get_cgv_released_movie, get_cgv_releasing_movie
from megabox_movie_info import get_megabox_upcoming_info, get_megabox_released_info
from lotte_movie_info import get_lotte_released_info, get_lotte_upcoming_info


def merge_movie_info(movie_info: list, movie_theater_info: list):
    return movie_info + movie_theater_info


def get_all_movie_info():
    # CGV, 메가박스, 롯데시네마에서 상영중인 영화 정보를 가져옴
    cgv_released_movies = get_cgv_released_movie()
    cgv_releasing_movies = get_cgv_releasing_movie()
    megabox_released_movies = get_megabox_released_info()
    megabox_upcoming_movies = get_megabox_upcoming_info()
    lotte_released_movies = get_lotte_released_info()
    lotte_upcoming_movies = get_lotte_upcoming_info()

    # 스크래핑한 영화 정보를 합침
    cgv_movie_names = merge_movie_info(cgv_released_movies, cgv_releasing_movies)
    megabox_movie_names = merge_movie_info(megabox_released_movies, megabox_upcoming_movies)
    lotte_movie_names = merge_movie_info(lotte_released_movies, lotte_upcoming_movies)

    return cgv_movie_names, megabox_movie_names, lotte_movie_names

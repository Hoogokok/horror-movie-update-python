import sched
import time
import logging

from update_ended_movies import update_ended
from update_movie_theaters import update_theaters_info
from update_upcoming_movie import update_upcoming_movie
from update_netflix_expiring_movie import update_netflix_expiring_movie
from find_all_movie_info import get_all_movie_info
from update_movie_provider import update_all_providers  # 새로 추가된 임포트

logging.basicConfig(level=logging.DEBUG)
s = sched.scheduler(time.time, time.sleep)
# 일주일 마다 실행
ONE_WEEK = 604800


def update_all_movie_info():
    cgv_movie_names, megabox_movie_names, lotte_movie_names = get_all_movie_info()
    return cgv_movie_names, megabox_movie_names, lotte_movie_names


def update_scheduler(sc):
    update_netflix_expiring_movie()
    cgv_movie_names, megabox_movie_names, lotte_movie_names = update_all_movie_info()
    update_theaters_info(cgv_movie_names, megabox_movie_names, lotte_movie_names)
    update_ended(cgv_movie_names, megabox_movie_names, lotte_movie_names)
    update_upcoming_movie()
    update_all_providers()  # 영화 공급자 정보 업데이트
    s.enter(ONE_WEEK, 1, update_scheduler, (sc,))


def main():
    while True:
        try:
            s.enter(0, 1, update_scheduler, (s,))
            s.run()
        except Exception as e:
            logging.error(e)
            logging.info("한 시간 후 다시 실행")
            # 한 시간 후 다시 실행
            time.sleep(3600)


if __name__ == '__main__':
    main()

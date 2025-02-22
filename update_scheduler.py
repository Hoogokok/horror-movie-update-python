import asyncio
from logging_config import setup_logging, get_logger
from functools import partial
from update_ended_movies import update_ended
from update_movie_theaters import update_theaters_info
from update_upcoming_movie import update_upcoming_movie
from update_movie_provider import update_all_providers
from update_netflix_expiring_movie import update_netflix_expiring_movie
from find_all_movie_info import get_all_movie_info

setup_logging()
logger = get_logger(__name__)

ONE_WEEK = 604800
MAX_CONCURRENT_TASKS = 3  # 동시에 실행할 수 있는 최대 작업 수

async def update_all_movie_info():
    return await get_all_movie_info()

async def run_with_semaphore(semaphore, func, *args):
    async with semaphore:
        return await func(*args)

async def update_scheduler():
    try:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
        
        # 병렬로 실행할 수 있는 작업들을 동시에 시작
        tasks = [
            run_with_semaphore(semaphore, update_upcoming_movie),
            run_with_semaphore(semaphore, update_netflix_expiring_movie),
            run_with_semaphore(semaphore, update_all_movie_info),
            run_with_semaphore(semaphore, update_all_providers),
        ]

        # 모든 작업이 완료될 때까지 기다림
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # 예외 처리 및 결과 추출
        cgv_movie_names, megabox_movie_names, lotte_movie_names = None, None, None
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"작업 {tasks[i].__name__} 실행 중 오류 발생: {result}")
            elif i == 2:  # update_all_movie_info의 결과
                cgv_movie_names, lotte_movie_names = result

        if cgv_movie_names is None or lotte_movie_names is None:
            logger.error("영화 정보를 가져오는데 실패했습니다.")
            return

        # 순차적으로 실행해야 하는 작업들
        await run_with_semaphore(semaphore, update_theaters_info, cgv_movie_names, lotte_movie_names)
        await run_with_semaphore(semaphore, update_ended, cgv_movie_names, lotte_movie_names)

        logger.info("모든 업데이트 작업이 완료되었습니다.")
    except Exception as e:
        logger.exception(f"업데이트 중 오류 발생: {e}")

async def main():
    while True:
        try:
            await update_scheduler()
            await asyncio.sleep(ONE_WEEK)
        except Exception as e:
            logger.exception(f"예상치 못한 오류 발생: {e}")
            logger.info("한 시간 후 다시 실행")
            await asyncio.sleep(3600)

if __name__ == '__main__':
    asyncio.run(main())

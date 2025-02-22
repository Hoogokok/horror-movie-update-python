import asyncio
import aiohttp
import traceback
import time
from typing import List, Dict, Any, Tuple
from logging_config import setup_logging, get_logger
from database import get_db_pool, execute_many, batch_insert
from config import (
    BASE_URL, HORROR_GENRE_ID, PROVIDER_MAPPING, HEADERS
)
from aiocache import cached, Cache
from aiocache.serializers import PickleSerializer
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from contextlib import asynccontextmanager
from datetime import datetime
import asyncpg

# 스크립트 시작 시 로깅 설정 초기화
setup_logging()
logger = get_logger(__name__)

MAX_DB_CONNECTIONS = 5
db_semaphore = asyncio.Semaphore(MAX_DB_CONNECTIONS)

def create_url_with_params(base_url: str, params: Dict[str, Any]) -> str:
    return f"{base_url}?{urlencode(params)}"

def update_url_with_page(url: str, page: int) -> str:
    parsed_url = urlparse(url)
    query_params = dict(parse_qsl(parsed_url.query))
    query_params['page'] = str(page)
    new_query = urlencode(query_params)
    return urlunparse(parsed_url._replace(query=new_query))

@asynccontextmanager
async def get_db_connection():
    pool = await get_db_pool()
    try:
        yield pool
    finally:
        await pool.close()

async def upsert_movies(conn, movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    async with db_semaphore:
        temp_table_name = 'temp_movies'
        
        movie_data = [
            (int(movie['the_movie_db_id']), movie['title'], movie['release_date'], movie['overview'], movie['poster_path'], movie['is_theatrical_release'])
            for movie in movies
        ]
        
        try:
            async with conn.transaction():
                # 임시 테이블이 존재하면 삭제
                await conn.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
                
                # 임시 테이블 생성
                await conn.execute(f'''
                    CREATE TEMP TABLE {temp_table_name} (
                        the_movie_db_id INTEGER PRIMARY KEY,
                        title TEXT,
                        release_date DATE,
                        overview TEXT,
                        poster_path TEXT,
                        is_theatrical_release BOOLEAN
                    )
                ''')
                
                # 임시 테이블에 데이터 삽입
                await conn.copy_records_to_table(temp_table_name, records=movie_data)
                
                # 실제 테이블에 upsert 수행
                result = await conn.fetch(f'''
                    INSERT INTO movie (the_movie_db_id, title, release_date, overview, poster_path, is_theatrical_release)
                    SELECT * FROM {temp_table_name}
                    ON CONFLICT (the_movie_db_id) DO UPDATE
                    SET title = EXCLUDED.title,
                        release_date = EXCLUDED.release_date,
                        overview = EXCLUDED.overview,
                        poster_path = EXCLUDED.poster_path,
                        is_theatrical_release = EXCLUDED.is_theatrical_release
                    RETURNING id, the_movie_db_id, title
                ''')
                
                # 임시 테이블 삭제
                await conn.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
                
                logger.info(f"{len(result)}개의 영화 업서트 완료")
                return [dict(row) for row in result]
                
        except Exception as e:
            logger.error(f"영화 업서트 중 오류 발생: {e}")
            # 오류 발생 시에도 임시 테이블 삭제 시도
            try:
                await conn.execute(f'DROP TABLE IF EXISTS {temp_table_name}')
            except Exception:
                pass
            raise

async def get_discover_movie_all_pages(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> List[Dict[str, Any]]:
    logger.info(f"요청 헤더: {headers}")
    async def fetch_page(page: int) -> Dict[str, Any]:
        new_url = update_url_with_page(url, page)
        async with session.get(new_url, headers=headers) as response:
            if response.status != 200:
                logger.error(f"URL에 대한 HTTP 오류 {response.status}: {new_url}")
                return {}
            data = await response.json()
            logger.info(f"페이지 {page} 조회 완료, {len(data.get('results', []))}개의 결과")
            return data

    current_page, total_pages, upcoming_movies = await get_first_page_discover_movie(session, url, headers)
    logger.info(f"전체 페이지 수: {total_pages}")
    
    if total_pages > 1:
        tasks = [fetch_page(page) for page in range(current_page + 1, total_pages + 1)]
        responses = await asyncio.gather(*tasks)
        
        for response in responses:
            if response:  # 빈 딕셔너리가 아닌 경우에만 처리
                upcoming_movies.extend(map_discover_movie_data(response.get('results', [])))
    
    logger.info(f"전체 조회된 영화 수: {len(upcoming_movies)}")
    return upcoming_movies

async def get_first_page_discover_movie(session: aiohttp.ClientSession, url: str, headers: Dict[str, str]) -> Tuple[int, int, List[Dict[str, Any]]]:
    async with session.get(url, headers=headers) as response:
        data = await response.json()
        logger.info(f"API 응답: {data}")
        logger.debug(f"첫 번째 영화 결과: {data.get('results', [])[0] if data.get('results') else '결과 없음'}")
        current_page = data.get('page', 0)
        total_pages = data.get('total_pages', 0)
        results = data.get('results', [])
        upcoming_movies = map_discover_movie_data(results)
        logger.info(f"첫 페이지에서 {len(upcoming_movies)}개의 영화 파싱 완료")
        return current_page, total_pages, upcoming_movies

def map_discover_movie_data(upcoming_movies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    horrors = []
    for movie in upcoming_movies:
        release_date_str = movie.get('release_date', '')
        try:
            release_date = datetime.strptime(release_date_str, '%Y-%m-%d').date() if release_date_str else None
        except ValueError:
            logger.warning(f"Invalid release date format for movie {movie.get('title')}: {release_date_str}")
            release_date = None

        horrors.append({
            "title": movie.get('title', ''),
            "release_date": release_date,
            "overview": movie.get('overview', ''),
            "poster_path": movie.get('poster_path', ''),
            "the_movie_db_id": int(movie.get('id', 0)),
            "is_theatrical_release": False
        })
    logger.info(f"Filtered {len(horrors)} horror movies from {len(upcoming_movies)} total movies")
    return horrors

@cached(ttl=3600, cache=Cache.MEMORY, serializer=PickleSerializer())
async def get_movies_for_provider(session: aiohttp.ClientSession, provider_id: int) -> List[Dict[str, Any]]:
    params = {
        'include_adult': 'false',
        'include_video': 'false',
        'language': 'ko-KR',
        'page': 1,
        'sort_by': 'popularity.desc',
        'watch_region': 'KR',
        'with_genres': str(HORROR_GENRE_ID),
        'with_watch_providers': str(provider_id)
    }
    url = create_url_with_params(BASE_URL, params)
    logger.info(f"Fetching movies for provider ID {provider_id} with URL: {url}")
    try:
        movies = await get_discover_movie_all_pages(session, url, HEADERS)
        logger.info(f"Retrieved {len(movies)} movies for provider ID {provider_id}")
        return movies
    except Exception as e:
        logger.error(f"Error fetching movies for provider ID {provider_id}: {e}")
        return []

async def insert_movies_with_provider(pool, movies: List[Dict[str, Any]], provider_id: int) -> None:
    mapped_provider_id = PROVIDER_MAPPING.get(provider_id)
    if not mapped_provider_id:
        logger.error(f"알 수 없는 공급자 ID: {provider_id}")
        return
    
    logger.info(f"공급자 ID {provider_id} (매핑된 ID: {mapped_provider_id})의 영화 {len(movies)}개를 처리 중입니다.")

    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                start_time = time.time()
                inserted_movies = await upsert_movies(conn, movies)
                upsert_time = time.time() - start_time
                logger.info(f"영화 업서트 완료. 소요 시간: {upsert_time:.2f}초")
                
                if inserted_movies:
                    movie_provider_data = [
                        (movie['id'], mapped_provider_id)
                        for movie in inserted_movies
                    ]
                    
                    start_time = time.time()
                    await upsert_movie_providers(conn, movie_provider_data)
                    insert_time = time.time() - start_time
                    logger.info(f"공급자 정보 업서트 완료. 소요 시간: {insert_time:.2f}초")
                    
                    logger.info(f"트랜잭션 커밋 완료")
                else:
                    logger.warning(f"공급자 ID {provider_id}에 대해 삽입된 영화가 없습니다.")
    except asyncpg.exceptions.PostgresError as e:
        logger.error(f"데이터베이스 오류 발생: {e}")
    except Exception as e:
        logger.error(f"예상치 못한 오류 발생: {e}")
        logger.error(traceback.format_exc())
    
    logger.info(f"공급자 ID {provider_id} (매핑된 ID: {mapped_provider_id})의 영화 {len(movies)}개 처리가 완료되었습니다.")

async def upsert_movie_providers(conn, movie_provider_data: List[Tuple[int, int]]) -> None:
    async with conn.transaction():
        await conn.execute('''
            CREATE TEMP TABLE temp_movie_providers (
                movie_id INTEGER,
                the_provider_id INTEGER
            ) ON COMMIT DROP
        ''')
        
        await conn.copy_records_to_table('temp_movie_providers', records=movie_provider_data)
        
        await conn.execute('''
            INSERT INTO movie_providers (movie_id, the_provider_id)
            SELECT movie_id, the_provider_id FROM temp_movie_providers
            ON CONFLICT (movie_id, the_provider_id) DO NOTHING
        ''')

async def update_all_providers() -> None:
    logger.info("영화 공급자 정보 업데이트 시작")
    start_time = time.time()
    
    async with aiohttp.ClientSession() as session, get_db_connection() as pool:
        async with pool.acquire() as conn:
            # 현재 공급자-영화 관계를 저장할 임시 테이블 생성
            await conn.execute('''
                DROP TABLE IF EXISTS current_movie_providers;
                CREATE TEMP TABLE current_movie_providers (
                    movie_id INTEGER,
                    the_provider_id INTEGER
                )
            ''')
            
            # 각 공급자별로 현재 제공 중인 영화 정보 수집
            for provider_id in PROVIDER_MAPPING.keys():
                try:
                    # 각 공급자마다 새로운 트랜잭션 사용
                    async with conn.transaction():
                        movies = await get_movies_for_provider(session, provider_id)
                        if movies:
                            inserted_movies = await upsert_movies(conn, movies)
                            mapped_provider_id = PROVIDER_MAPPING[provider_id]
                            
                            movie_provider_data = [
                                (movie['id'], mapped_provider_id)
                                for movie in inserted_movies
                            ]
                            
                            await conn.copy_records_to_table(
                                'current_movie_providers', 
                                records=movie_provider_data
                            )
                            logger.info(f"공급자 ID {provider_id}의 {len(movie_provider_data)}개 영화 관계 정보 추가 완료")
                            
                except Exception as e:
                    logger.error(f"공급자 ID {provider_id} 처리 중 오류 발생: {e}")
                    continue
            
            try:
                # 최종 업데이트는 별도 트랜잭션으로 처리
                async with conn.transaction():
                    # 더 이상 제공되지 않는 영화-공급자 관계 삭제
                    delete_result = await conn.execute('''
                        WITH to_delete AS (
                            SELECT mp.movie_id, mp.the_provider_id
                            FROM movie_providers mp
                            LEFT JOIN current_movie_providers cmp 
                                ON mp.movie_id = cmp.movie_id 
                                AND mp.the_provider_id = cmp.the_provider_id
                            WHERE cmp.movie_id IS NULL
                        )
                        DELETE FROM movie_providers mp
                        USING to_delete td
                        WHERE mp.movie_id = td.movie_id 
                        AND mp.the_provider_id = td.the_provider_id
                    ''')
                    logger.info(f"더 이상 제공되지 않는 {delete_result} 개의 영화-공급자 관계 삭제 완료")
                    
                    # 새로운 영화-공급자 관계 추가
                    insert_result = await conn.execute('''
                        INSERT INTO movie_providers (movie_id, the_provider_id)
                        SELECT DISTINCT cmp.movie_id, cmp.the_provider_id 
                        FROM current_movie_providers cmp
                        ON CONFLICT (movie_id, the_provider_id) DO NOTHING
                    ''')
                    logger.info(f"새로운 영화-공급자 관계 {insert_result} 개 추가 완료")
            
            finally:
                # 임시 테이블 정리
                await conn.execute('DROP TABLE IF EXISTS current_movie_providers')
    
    end_time = time.time()
    logger.info(f"모든 공급자 정보 업데이트 완료. 총 소요 시간: {end_time - start_time:.2f}초")
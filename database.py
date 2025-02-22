import asyncio
import asyncpg
from typing import List, Dict, Any, Tuple
from logging_config import get_logger
from config import DB_CONFIG

logger = get_logger(__name__)

pool = None

async def get_db_pool():
    global pool
    if not pool:
        try:
            pool = await asyncpg.create_pool(
                host=DB_CONFIG['host'],
                port=DB_CONFIG['port'],
                user=DB_CONFIG['user'],
                password=DB_CONFIG['password'],
                database=DB_CONFIG['database'],
                statement_cache_size=0,
                max_cached_statement_lifetime=0,
                server_settings={
                    'statement_timeout': '60000',
                    'prepared_statements': 'false'
                },
                command_timeout=60
            )
            logger.info("데이터베이스 연결 풀 생성 완료")
        except Exception as e:
            logger.error(f"데이터베이스 연결 풀 생성 중 오류 발생: {str(e)}")
            raise
    return pool

async def close_db_pool():
    global pool
    if pool:
        await pool.close()
        pool = None

async def execute_query(pool, query: str, *args) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        try:
            results = await conn.fetch(query, *args)
            return [dict(result) for result in results]
        except Exception as e:
            logger.error(f"쿼리 실행 중 오류 발생: {e}", extra={"query": query, "args": args})
            raise

async def execute_many(pool, query: str, args: List[Tuple]) -> List[Dict[str, Any]]:
    async with pool.acquire() as conn:
        try:
            results = await conn.fetch(query, *args)
            return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return []

async def execute_transaction(pool, queries: List[Dict[str, Any]]) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            for query in queries:
                await conn.execute(query['query'], *query.get('args', []))

async def batch_insert(pool, table_name: str, columns: List[str], records: List[Tuple]):
    async with pool.acquire() as conn:
        try:
            result = await conn.copy_records_to_table(
                table_name,
                records=records,
                columns=columns
            )
            return result
        except Exception as e:
            logger.error(f"Error batch inserting records: {e}")
            logger.error(f"Table: {table_name}, Columns: {columns}")
            logger.error(f"First record: {records[0] if records else 'No records'}")
            raise





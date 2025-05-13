import psycopg2
from psycopg2.extras import RealDictCursor

# 公共数据库连接函数
def get_pg_conn():
    return psycopg2.connect(**get_pg_conn_config())

# db.py
def get_pg_conn_config():
    return {
        "dbname": "test",
        "user": "postgres",
        "password": "lxw19980714",
        "host": "localhost",
        "port": 5432
    }

from datetime import datetime

# 通用插入函数
def insert_job_detail(job_run_id, job_name, custom_id):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            create_time = datetime.now()
            cur.execute(
                """
                INSERT INTO dataset_job_detail (job_run_id, job_name, id, create_time)
                VALUES (%s, %s, %s, %s)
                """,
                (job_run_id, job_name, custom_id, create_time)
            )
        conn.commit()
    finally:
        conn.close()

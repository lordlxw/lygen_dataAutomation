import psycopg2
from psycopg2.extras import RealDictCursor

# 公共数据库连接函数
def get_pg_conn():
    return psycopg2.connect(**get_pg_conn_config())

# db.py
def get_pg_conn_config():
    return {
        "host": "192.168.2.2",
        "port": 5432,
        "dbname": "SynapseFlow",
        "user": "user_pg",
        "password": "password_zYHcAJ",
        "connect_timeout": 5  # ✅ 新增这一行，单位是秒
    }


from datetime import datetime

# 通用插入函数
def insert_job_detail(job_run_id, job_name, custom_id, task_id):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            create_time = datetime.now()
            cur.execute(
                """
                INSERT INTO dataset_job_detail (job_run_id, job_name, id, task_id, create_time)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (job_run_id, job_name, custom_id, task_id, create_time)
            )
        conn.commit()
    finally:
        conn.close()


def insert_pdf_info(job_run_id, pdf_location):
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            create_time = datetime.now()
            cur.execute(
                """
                INSERT INTO pdf_info (run_id, original_pdf_location,layout_pdf_location, create_time)
                VALUES (%s, %s, %s, %s)
                """,
                (job_run_id, pdf_location,pdf_location, create_time)
            )
        conn.commit()
    finally:
        conn.close()


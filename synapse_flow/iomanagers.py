# io_managers.py
import os
import json
import sqlite3
import uuid  # 用于生成 UUID
import psycopg2.extras  # 导入 psycopg2 的 extras 模块来支持 UUID
from dagster import IOManager, io_manager
from synapse_flow.db import get_pg_conn,get_pg_conn_config
class JsonFileIOManager(IOManager):
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)

    def _get_path(self, context):
        return os.path.join(self.base_dir, f"{context.step_key}.json")

    def handle_output(self, context, obj):
        path = self._get_path(context)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
        context.log.info(f"写入 JSON 文件到: {path}")

    def load_input(self, context):
        upstream_output_context = context.upstream_output
        path = self._get_path(upstream_output_context)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        context.log.info(f"读取 JSON 文件: {path}")
        return data

@io_manager
def json_file_io_manager(_):
    return JsonFileIOManager(base_dir="storage/json_outputs")



class SQLiteIOManager(IOManager):
    def __init__(self, db_path="data.db"):
        self.db_path = db_path
        self._ensure_table_exists()

    def _ensure_table_exists(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS dagster_data (key TEXT PRIMARY KEY, value TEXT)"
            )

    def handle_output(self, context, obj):
        key = context.step_key
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "REPLACE INTO dagster_data (key, value) VALUES (?, ?)",
                (key, json.dumps(obj)),
            )

    def load_input(self, context):
        upstream_key = context.upstream_output.step_key
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "SELECT value FROM dagster_data WHERE key = ?", (upstream_key,)
            )
            row = cursor.fetchone()
            if row is None:
                raise Exception(f"No data found for key: {upstream_key}")
            return json.loads(row[0])


@io_manager
def sqlite_io_manager(_):
    return SQLiteIOManager()




from dagster import IOManager, io_manager
import psycopg2
from typing import Any
from datetime import datetime  # ✅ 导入 datetime

class PostgresIOManager(IOManager):
    def __init__(self, db_params: dict):
        self.db_params = db_params
    
    def handle_output(self, context, obj: Any):
        # 获取从op返回的数据 (包含文件内容)
        pdf_data = obj
        context.log.info(f"handle_output!!!")
        context.log.info(obj)
        # 与数据库连接
        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()
        create_time = datetime.now()  # ✅ 当前时间戳
        # 假设 run_id 是通过上下文动态传递的，或者根据需求生成
        run_id = context.step_context.run_id
  # 可以使用 run_id 作为 pdf_id，确保唯一性
        
        # 插入每条内容到数据库
        for item in pdf_data['content']:
            text = item['text']
            page = item['page']
            
            # 插入数据
            cursor.execute("""
            INSERT INTO pdf_json (run_id, text, text_level, type, page_index, create_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (run_id, text, 1, '正文', page, create_time))  # ✅ 添加时间戳
        
        # 提交事务并关闭连接
        connection.commit()
        cursor.close()
        connection.close()

    def load_input(self, context) -> Any:
        # 这里假设你是根据 run_id 获取数据
        run_id = context.step_context.run_id
  # 获取 pdf_id（可以是传递的参数）
        
        # 与数据库连接
        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()
        
        # 查询数据
        cursor.execute("""
            SELECT page_index, text
            FROM pdf_json
            WHERE run_id = %s
            ORDER BY page_index
        """, (run_id,))
        
        rows = cursor.fetchall()
        connection.close()

        # 将查询结果转换为返回的格式
        content = [{"page": row[0], "text": row[1]} for row in rows]
        return {
            "file": f"from-db-{run_id}",
            "content": content
        }

# IO Manager 注册
@io_manager
def postgres_io_manager(init_context):
    # db_params = {
    #     "dbname": "test",
    #     "user": "postgres",
    #     "password": "lxw19980714",
    #     "host": "localhost",  # 或者使用你的数据库主机
    #     "port": 5432         # PostgreSQL 默认端口
    # }
    db_params = get_pg_conn_config();
    return PostgresIOManager(db_params)




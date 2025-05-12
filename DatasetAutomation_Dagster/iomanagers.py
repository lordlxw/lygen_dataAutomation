# io_managers.py
import os
import json
import sqlite3
import uuid  # 用于生成 UUID
import psycopg2.extras  # 导入 psycopg2 的 extras 模块来支持 UUID
from dagster import IOManager, io_manager

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

class PostgresIOManager(IOManager):
    def __init__(self, db_params: dict):
        self.db_params = db_params
    
    def handle_output(self, context, obj: Any):
        pdf_data = obj
        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()
        
        for item in pdf_data['content']:
            text = item['text']
            page = item['page']
            pdf_id = 123  # Example PDF ID, you can dynamically generate or pass it
            
            # Insert the data into PostgreSQL
            cursor.execute("""
                INSERT INTO pdf_json (pdf_id, text, text_level, type, page_index)
                VALUES (%s, %s, %s, %s, %s)
            """, (pdf_id, text, 1, '正文', page))  # No need to insert ID if it's auto-generated
            
        connection.commit()
        cursor.close()
        connection.close()

    def load_input(self, context) -> Any:
        # 假设你知道要查的 pdf_id 是 123（你也可以通过 context.metadata 传）
        pdf_id = 123

        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()

        cursor.execute("""
            SELECT page_index, text
            FROM pdf_json
            WHERE pdf_id = %s
            ORDER BY page_index
        """, (pdf_id,))
        
        rows = cursor.fetchall()
        connection.close()

        # 转换成上游返回的数据格式
        content = [{"page": row[0], "text": row[1]} for row in rows]
        return {
            "file": f"from-db-{pdf_id}",
            "content": content
        }


# IO Manager 注册
@io_manager
def postgres_io_manager(init_context):
    db_params = {
        "dbname": "test",
        "user": "postgres",
        "password": "lxw19980714",
        "host": "localhost",  # Or your DB host
        "port": 5432         # Default PostgreSQL port
    }
    return PostgresIOManager(db_params)




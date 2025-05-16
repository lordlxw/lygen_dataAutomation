from datetime import datetime
from synapse_flow.db import get_pg_conn

def insert_pdf_text_contents(run_id: str, contents: list):
    """
    批量插入多条 PDF 文本内容，整批内容共用同一个 version。
    contents 是一个 list，每个元素是一个 dict，对应 PdfTextContent。
    新增支持传入 BlockIndex。
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            # 1. 查询当前最大 version
            cur.execute("""
                SELECT COALESCE(MAX(version), 0) FROM pdf_json WHERE run_id = %s
            """, (run_id,))
            max_version = cur.fetchone()[0]
            new_version = max_version + 1
            create_time = datetime.now()

            # 2. 批量插入，统一 version
            for item in contents:
                text = item.get("Text", "")
                page_index = item.get("PageIndex", 0)
                text_level = item.get("TextLevel", 0)
                type_ = item.get("Type", "")
                block_index = item.get("BlockIndex")  # 新增字段
                create_time_str = item.get("CreateTime")

                # 优先使用传入的 CreateTime，否则用当前时间
                if create_time_str:
                    try:
                        create_time = datetime.fromisoformat(create_time_str)
                    except Exception:
                        create_time = datetime.now()

                cur.execute("""
                    INSERT INTO pdf_json (run_id, text, page_index, text_level, create_time, version, type, block_index)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (run_id, text, page_index, text_level, create_time, new_version, type_, block_index))

        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()



def query_pdf_text_contents(run_id: str, version: int) -> list:
    """
    根据 run_id 和 version 查询对应的 PDF 文本内容列表。
    返回列表，每个元素是 dict，包含对应字段。
    """
    conn = get_pg_conn()
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_id, text, page_index, text_level, create_time, version, type, block_index
                FROM pdf_json
                WHERE run_id = %s AND version = %s
                ORDER BY page_index ASC, block_index ASC
            """, (run_id, version))

            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "run_id": row[0],
                    "text": row[1],
                    "page_index": row[2],
                    "text_level": row[3],
                    "create_time": row[4].isoformat() if row[4] else None,
                    "version": row[5],
                    "type": row[6],
                    "block_index": row[7]
                })
    finally:
        conn.close()
    return results



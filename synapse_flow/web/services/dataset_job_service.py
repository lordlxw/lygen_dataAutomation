from datetime import datetime
from synapse_flow.db import get_pg_conn

def insert_pdf_text_contents(run_id: str, contents: list, based_version: int = None) -> int:
    """
    批量插入多条 PDF 文本内容，整批内容共用同一个 version。
    返回新生成的 version。
    """
    print("insert_pdf_text_contents")
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            # 1. 查询当前最大 version
            cur.execute("""
                SELECT COALESCE(MAX(version), 0) FROM pdf_json WHERE run_id = %s
            """, (run_id,))
            max_version = cur.fetchone()[0]
            new_version = max_version + 1

            # 2. 批量插入，统一 version + based_version
            for item in contents:
                text = item.get("text", "")
                page_index = item.get("page_index", 0)
                text_level = item.get("text_level", 0)
                type_ = item.get("type", "")
                block_index = item.get("block_index")
                is_title_marked = item.get("is_title_marked", False)
                exclude_from_finetune = item.get("exclude_from_finetune", False)
                remark = item.get("remark", "")
                original_text = item.get("original_text", text)  # 如果没有提供original_text，使用text作为默认值
                create_time = datetime.now()

                cur.execute("""
                    INSERT INTO pdf_json (
                        run_id, text, page_index, text_level, create_time,
                        version, type, block_index, based_version,
                        is_title_marked, exclude_from_finetune, remark, original_text
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    run_id, text, page_index, text_level, create_time,
                    new_version, type_, block_index, based_version,
                    is_title_marked, exclude_from_finetune, remark, original_text
                ))

        conn.commit()
        return new_version
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
                SELECT id, run_id, text, page_index, text_level, create_time,
                       version, type, block_index, is_title_marked,
                       based_version, exclude_from_finetune, remark, original_text
                FROM pdf_json
                WHERE run_id = %s AND version = %s
                ORDER BY page_index ASC, block_index ASC
            """, (run_id, version))

            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "id": row[0],
                    "run_id": row[1],
                    "text": row[2],
                    "page_index": row[3],
                    "text_level": row[4],
                    "create_time": row[5].isoformat() if row[5] else None,
                    "version": row[6],
                    "type": row[7],
                    "block_index": row[8],
                    "is_title_marked": False if row[9] is None else row[9],
                    "based_version": row[10],
                    "exclude_from_finetune": False if row[11] is None else row[11],
                    "remark": row[12] if len(row) > 12 else "",
                    "original_text": row[13] if len(row) > 13 else ""
                })
    finally:
        conn.close()
    return results






def query_versions_by_run_id(run_id: str) -> list:
    """
    查询某个 run_id 下的所有版本及其创建时间（每个版本最早的 create_time）。
    返回格式：
    [
        {"version": 1, "create_time": "2024-01-01T12:00:00"},
        {"version": 2, "create_time": "2024-01-02T14:30:00"},
        ...
    ]
    """
    conn = get_pg_conn()
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT version, MIN(create_time) AS create_time
                FROM pdf_json
                WHERE run_id = %s
                GROUP BY version
                ORDER BY version Desc
            """, (run_id,))

            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "version": row[0],
                    "create_time": row[1].isoformat() if row[1] else None
                })
    finally:
        conn.close()
    return results


def query_all_pdf_infos() -> list:
    """
    查询 pdf_info 表中所有记录，返回 original_pdf_name、create_time 和 run_id 字段。
    返回格式：
    [
        {
            "original_pdf_name": "xxx.pdf",
            "create_time": "2024-01-01T12:00:00",
            "run_id": "xxxx-xxxx-xxxx"
        },
        ...
    ]
    """
    conn = get_pg_conn()
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT original_pdf_name, create_time, run_id
                FROM pdf_info
                ORDER BY create_time DESC
                LIMIT 20
            """)
            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "original_pdf_name": row[0],
                    "create_time": row[1].isoformat() if row[1] else None,
                    "run_id": row[2]
                })
    finally:
        conn.close()
    return results

def query_pdf_infos_by_user_id(user_id: str) -> list:
    """
    根据 user_id 查询 pdf_info 表中对应的记录，返回 original_pdf_name、create_time 和 run_id 字段。
    返回格式：
    [
        {
            "original_pdf_name": "xxx.pdf",
            "create_time": "2024-01-01T12:00:00",
            "run_id": "xxxx-xxxx-xxxx"
        },
        ...
    ]
    """
    conn = get_pg_conn()
    results = []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT original_pdf_name, create_time, run_id
                FROM pdf_info
                WHERE user_id = %s
                ORDER BY create_time DESC
                LIMIT 20
            """, (user_id,))
            rows = cur.fetchall()
            for row in rows:
                results.append({
                    "original_pdf_name": row[0],
                    "create_time": row[1].isoformat() if row[1] else None,
                    "run_id": row[2]
                })
    finally:
        conn.close()
    return results





def insert_change_log(run_id: str, version: int, change_json_log: str) -> bool:
    """
    插入变更日志到 pdf_change_log 表。
    返回是否成功。
    """
    if not run_id or not change_json_log:
        return False

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            create_time = datetime.now()
            cur.execute("""
                INSERT INTO pdf_change_log (run_id, version, change_json_log, create_time)
                VALUES (%s, %s, %s, %s)
            """, (run_id, version, change_json_log, create_time))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"insert_change_log error: {e}")
        return False
    finally:
        conn.close()

import json
def query_change_log(run_id: str, version: int) -> dict:
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT run_id, version, change_json_log, create_time
                FROM pdf_change_log
                WHERE run_id = %s AND version = %s
                LIMIT 1
            """, (run_id, version))
            row = cur.fetchone()
            if not row:
                return None
            # row[2] 是字符串，需要转成列表对象
            change_json_log = row[2]
            try:
                # 尝试转成列表或字典
                change_json_log_obj = json.loads(change_json_log)
            except Exception:
                # 转换失败保持原样
                change_json_log_obj = change_json_log

            return {
                "run_id": row[0],
                "version": row[1],
                "change_json_log": change_json_log_obj,
                "create_time": row[3].isoformat() if row[3] else None
            }
    finally:
        conn.close()



def query_based_version(run_id: str, version: int) -> int:
    """
    查询指定 run_id 和 version 的记录对应的 based_version。
    如果不存在，返回 None。
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT based_version
                FROM pdf_json
                WHERE run_id = %s AND version = %s
                LIMIT 1
            """, (run_id, version))
            row = cur.fetchone()
            if row:
                return row[0]
            return None
    finally:
        conn.close()


def update_user_id_by_run_id(run_id: str, new_user_id: str) -> bool:
    """
    根据 run_id 更新对应记录的 user_id 字段。
    返回是否更新成功。
    """
    if not run_id or not new_user_id:
        return False

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE pdf_info
                SET user_id = %s
                WHERE run_id = %s
            """, (new_user_id, run_id))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"update_user_id_by_run_id error: {e}")
        return False
    finally:
        conn.close()

def get_based_version(run_id: str, version: int) -> int | None:
    """
    根据 run_id 和 version 查询对应的 based_version。
    如果未找到记录，返回 None。
    """
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT based_version 
                FROM pdf_json 
                WHERE run_id = %s AND version = %s
                LIMIT 1
            """, (run_id, version))
            result = cur.fetchone()
            if result:
                return result[0]
            else:
                return None
    finally:
        conn.close()

def get_version_chain(run_id: str, version: int) -> list[int]:
    """
    获取从基础版本0开始，直到传入版本的完整版本链。
    例如：传入 version=5，链可能是 [0, 2, 4, 5]。
    """
    if version == 0:
        return [0]

    chain = []
    current_version = int(version)

    while current_version != 0:
        chain.append(current_version)
        based_version = get_based_version(run_id, current_version)
        if based_version is None:
            # 数据异常，跳出循环避免死循环
            break
        current_version = int(based_version)

    # 最后加上基版本0
    if 0 not in chain:
        chain.append(0)

    chain.reverse()
    return chain


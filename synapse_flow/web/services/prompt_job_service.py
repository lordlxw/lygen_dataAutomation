# 针对指示词封装的service，凯铭用
from synapse_flow.db import get_pg_conn
# 获取所有任务
def split_text():
        print("split_text")
        return


def get_api_key(key_name="openai"):
    """
    获取指定 key_name 的最新可用 API Key，默认是 'openai'

    Args:
        key_name (str): API key 的名称

    Returns:
        str or None: 找到则返回 API key 字符串，否则返回 None
    """
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()

        sql = """
            SELECT api_key 
            FROM openapi_keys 
            WHERE status = 1 AND key_name = %s 
            ORDER BY updated_at DESC 
            LIMIT 1;
        """
        cursor.execute(sql, (key_name,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            print(f"⚠️ 未找到 key_name = '{key_name}' 的可用 API Key")
            return None

    except Exception as e:
        print(f"❌ 查询 API Key 出错: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

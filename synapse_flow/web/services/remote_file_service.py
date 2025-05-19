# 远程文件获取
from synapse_flow.db import get_pg_conn



# 创建新任务
from synapse_flow.db import get_pg_conn

def getPdfByRunningId(runningId):
    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        sql = """
        SELECT layout_pdf_location
        FROM pdf_info
        WHERE run_id = %s
        ORDER BY create_time DESC
        LIMIT 1
        """
        cursor.execute(sql, (runningId,))
        result = cursor.fetchone()
        if result:
            return result[0]  # layout_pdf_location字段
        else:
            return None  # 找不到对应记录时返回None
    except Exception as e:
        print(f"查询出错: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

        
       

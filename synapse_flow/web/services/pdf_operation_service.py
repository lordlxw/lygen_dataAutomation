from synapse_flow.db import get_pg_conn

def getAllPdfInfos():
    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        sql = """
        SELECT 
            id,
            run_id,
            original_pdf_location,
            layout_pdf_location,
            create_time
        FROM pdf_info
        ORDER BY create_time DESC;
        """
        cursor.execute(sql)
        rows = cursor.fetchall()
        result = []
        for row in rows:
            result.append({
                "id": row[0],
                "run_id": row[1],
                "original_pdf_location": row[2],
                "layout_pdf_location": row[3],
                "create_time": row[4].isoformat() if row[4] else None
            })
        return result
    except Exception as e:
        print(f"查询出错: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

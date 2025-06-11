from synapse_flow.db import get_pg_conn
import base64
from io import BytesIO
from pdf2image import convert_from_bytes
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


def convert_pdf_to_images(pdf_bytes: bytes):
    try:
        images = convert_from_bytes(pdf_bytes, dpi=200)
        result_pages = []
        for index, image in enumerate(images):
            buffered = BytesIO()
            image.save(buffered, format="PNG")
            img_base64 = base64.b64encode(buffered.getvalue()).decode("utf-8")
            result_pages.append({
                "page_index": index,
                "image_base64": f"data:image/png;base64,{img_base64}"
            })
        return {
            "code": "00000",
            "message": "转换成功",
            "value": {
                "pages": result_pages
            }
        }
    except Exception as e:
        return {
            "code": "00003",
            "message": f"转换失败: {str(e)}",
            "value": None
        }
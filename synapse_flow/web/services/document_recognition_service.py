from synapse_flow.documentRecognitionJob import document_recognition_pipeline  # 确保导入正确
from pathlib import Path
from datetime import datetime
import shutil
from synapse_flow.iomanagers import postgres_io_manager
from synapse_flow.db import get_pg_conn
import json
from typing import Dict, Any
# 其他import ...

UPLOAD_ROOT = Path("uploads")
def save_upload_file(file) -> Path:
    # 先存到临时目录
    tmp_dir = UPLOAD_ROOT / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)

    filename = f"{Path(file.filename).stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}{Path(file.filename).suffix}"
    file_path = tmp_dir / filename
    file.save(file_path)
    return file_path

def backup_file(file_path: Path, run_id: str) -> Path:
    ext = file_path.suffix.lower()
    file_type_dir = {
        '.png': 'png',
        '.jpg': 'png',
        '.pdf': 'pdf',
        '.txt': 'txt',
        '.xlsx': 'xlsx',
        '.csv': 'csv',
    }.get(ext, 'others')

    target_dir = UPLOAD_ROOT / file_type_dir / run_id
    target_dir.mkdir(parents=True, exist_ok=True)

    target_path = target_dir / file_path.name
    shutil.copy2(file_path, target_path)
    return target_path

def run_document_recognition(file_path: Path):
    print("run_document_recognition")
    result = document_recognition_pipeline.execute_in_process(
    run_config={
        "ops": {
            "read_file": {
                "inputs": {
                    "file_path": str(file_path)
                }
            }
        },
        "resources": {
            "postgres_io_manager": {
                "config": {}
            }
        },
        "execution": {
            "config": {}
        }
    },
    resources={
        "postgres_io_manager": postgres_io_manager
    }
    )

    run_id = result.run_id
    print("backup_file—__1",run_id);
    # 调用备份方法
    backup_path = backup_file(file_path, run_id)
    print("backup_file—__2",file_path);
    print("backup_file—__3",backup_path);

    return {
        "success": result.success,
        "run_id": run_id,
        "backup_path": str(backup_path)
    }



def get_invoice_data_by_run_id(run_id: str) -> Dict[str, Any]:
    """
    根据 run_id 查询 invoice_main 和对应的 invoice_detail 详细数据
    返回格式：
    {
        "main": { ... invoice_main字段 ... },
        "details": [ {... invoice_detail 字段 ... }, {...} ]
    }
    """
    main_fields = [
        "id",
        "invoice_code",
        "invoice_number",
        "printed_invoice_code",
        "printed_invoice_number",
        "invoice_date",
        "machine_code",
        "check_code",
        "purchaser_name",
        "purchaser_tax_number",
        "purchaser_contact_info",
        "purchaser_bank_account",
        "password_area",
        "invoice_amount_pre_tax",
        "invoice_tax",
        "total_amount_in_words",
        "total_amount",
        "seller_name",
        "seller_tax_number",
        "seller_contact_info",
        "seller_bank_account",
        "recipient",
        "reviewer",
        "drawer",
        "remarks",
        "title",
        "form_type",
        "invoice_type",
        "special_tag",
        "created_at",
        "run_id",
        "create_time"
    ]

    detail_fields = [
        "id",
        "invoice_id",
        "item_name",
        "specification",
        "unit",
        "quantity",
        "unit_price",
        "amount",
        "tax_rate",
        "tax",
        "create_time"
    ]

    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            # 查询主表数据
            cur.execute(
                f"SELECT {', '.join(main_fields)} FROM invoice_main WHERE run_id = %s",
                (run_id,)
            )
            main_row = cur.fetchone()
            if not main_row:
                return {}  # 或抛异常、返回 None 表示没找到

            main_data = dict(zip(main_fields, main_row))

            # 查询子表明细数据
            cur.execute(
                f"SELECT {', '.join(detail_fields)} FROM invoice_detail WHERE invoice_id = %s ORDER BY id",
                (main_data['id'],)
            )
            detail_rows = cur.fetchall()
            details = [dict(zip(detail_fields, row)) for row in detail_rows]
            main_data["details"] = details
            return {
                "main": main_data
            } 
    finally:
        conn.close()

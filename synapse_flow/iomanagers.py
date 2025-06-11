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
    

    def handleInvoiceInfo(self, data, run_id=None):
        print("处理发票数据")
        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()
        print(data)
        # 提取发票 data 部分
        data = data["ocr_result"]["subMsgs"][0]["result"]["data"]
        try:
            create_time = datetime.now()
            if not run_id:
                run_id = "unknown_run_id"

            # 插入主表数据，新增 created_at 字段改为 create_time，与你要求一致
            sql_main = """
                INSERT INTO invoice_main (
                    invoice_code, invoice_number, printed_invoice_code, printed_invoice_number,
                    invoice_date, machine_code, check_code, purchaser_name, purchaser_tax_number,
                    purchaser_contact_info, purchaser_bank_account, password_area,
                    invoice_amount_pre_tax, invoice_tax, total_amount_in_words, total_amount,
                    seller_name, seller_tax_number, seller_contact_info, seller_bank_account,
                    recipient, reviewer, drawer, remarks, title, form_type,
                    invoice_type, special_tag, create_time, run_id
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                ) RETURNING id
            """

            invoice_date_str = data.get("invoiceDate", "")
            invoice_date = None
            if invoice_date_str:
                try:
                    invoice_date = datetime.strptime(invoice_date_str, "%Y年%m月%d日").date()
                except Exception:
                    invoice_date = None

            main_values = (
                data.get("invoiceCode"),
                data.get("invoiceNumber"),
                data.get("printedInvoiceCode"),
                data.get("printedInvoiceNumber"),
                invoice_date,
                data.get("machineCode"),
                data.get("checkCode"),
                data.get("purchaserName"),
                data.get("purchaserTaxNumber"),
                data.get("purchaserContactInfo"),
                data.get("purchaserBankAccountInfo"),
                data.get("passwordArea"),
                data.get("invoiceAmountPreTax"),
                data.get("invoiceTax"),
                data.get("totalAmountInWords"),
                data.get("totalAmount"),
                data.get("sellerName"),
                data.get("sellerTaxNumber"),
                data.get("sellerContactInfo"),
                data.get("sellerBankAccountInfo"),
                data.get("recipient"),
                data.get("reviewer"),
                data.get("drawer"),
                data.get("remarks"),
                data.get("title"),
                data.get("formType"),
                data.get("invoiceType"),
                data.get("specialTag"),
                create_time,  # 主表 create_time
                run_id
            )

            cursor.execute(sql_main, main_values)
            invoice_main_id = cursor.fetchone()[0]

            # 插入明细表数据，增加 create_time 字段
            details = data.get("invoiceDetails", [])
            sql_detail = """
                INSERT INTO invoice_detail (
                    invoice_id, item_name, specification, unit,
                    quantity, unit_price, amount, tax_rate, tax, create_time
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """
            for detail in details:
                detail_values = (
                    invoice_main_id,
                    detail.get("itemName"),
                    detail.get("specification"),
                    detail.get("unit"),
                    detail.get("quantity"),
                    detail.get("unitPrice"),
                    detail.get("amount"),
                    detail.get("taxRate"),
                    detail.get("tax"),
                    create_time  # 明细表 create_time
                )
                cursor.execute(sql_detail, detail_values)

            connection.commit()
            print(f"Invoice {invoice_main_id} and details saved successfully.")

        except Exception as e:
            connection.rollback()
            print(f"Error saving invoice info: {e}")
        finally:
            cursor.close()
            connection.close()


    def handle_output(self, context, obj: Any):
        data = obj
        run_id = context.step_context.run_id

        # 根据 type 字段走不同分支
        data_type = data.get("type")
        if data_type == "image":
            self.handleInvoiceInfo(data, run_id)  # 正确调用类内部方法
            return

        context.log.info(f"handle_output!!!")
        context.log.info(context)
        context.log.info(obj)

        connection = psycopg2.connect(**self.db_params)
        cursor = connection.cursor()
        create_time = datetime.now()
        context.log.info(f"handle_output run_id: {run_id}")

        # 用于记录每个 page 的 block_index 累加器
        page_block_counter = {}

        for item in data.get('content', []):
            text = item.get('text', '')
            item_type = item.get('type', '正文')  # 默认类型为 '正文'
            page = item.get('page', 0)
            block_index = page_block_counter.get(page, 0)

            cursor.execute("""
                INSERT INTO pdf_json (run_id, text, text_level, type, page_index, block_index, create_time, version, original_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (run_id, text, 1, item_type, page, block_index, create_time, 0, text))

            page_block_counter[page] = block_index + 1

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



# 其他持久化实现


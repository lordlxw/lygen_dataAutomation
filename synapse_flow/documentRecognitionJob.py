import os
import pandas as pd
from dagster import op, job, In, Out
from .functions.ocr_utils import ocr_image_to_json  
# ---------- Step 1: 读取文件与识别格式 ----------
@op(out={"file_info": Out(dict)})
def read_file(context, file_path: str):
    ext = os.path.splitext(file_path)[1].lower()
    context.log.info(f"[ReadFile] 读取文件: {file_path}，格式: {ext}")
    return {"path": file_path, "ext": ext}


# ---------- Step 2: 分发处理 ----------
@op(ins={"file_info": In(dict)}, out={"result": Out(dict)})
def dispatch_process(context, file_info):
    ext = file_info["ext"]
    path = file_info["path"]

    if ext == ".pdf":
        return handle_pdf(context, path)
    elif ext in [".png", ".jpg", ".jpeg"]:
        return handle_image(context, path)
    elif ext == ".txt":
        return handle_txt(context, path)
    elif ext in [".csv", ".xlsx"]:
        return handle_tabular(context, path)
    else:
        raise Exception(f"不支持的文件类型: {ext}")


# ---------- Step 3: 各格式处理策略 ----------
def handle_pdf(context, path: str) -> dict:
    context.log.info(f"[PDF] 处理 PDF 文件: {path}")
    # 示例：只返回路径
    return {"type": "pdf", "path": path, "summary": "PDF 文件处理完成"}


def handle_image(context, path: str) -> dict:
    context.log.info(f"[Image] 开始 OCR 识别: {path}")
    
    try:
        ocr_result = ocr_image_to_json(path)
        context.log.info(f"[Image] OCR 完成，字段数量: {len(ocr_result)}")
        print("ocr_result")
        print(ocr_result)
        data = {
            "type": "image",
            "path": path,
            "ocr_result": ocr_result,
            "summary": "图像 OCR 处理完成"
        }
        return data;
    except Exception as e:
        context.log.error(f"OCR 识别失败: {str(e)}")
        return {
            "type": "image",
            "path": path,
            "error": str(e),
            "summary": "图像 OCR 处理失败"
        }




def handle_txt(context, path: str) -> dict:
    context.log.info(f"[TXT] 读取文本文件: {path}")
    with open(path, encoding='utf-8') as f:
        text = f.read()
    return {"type": "txt", "path": path, "content_preview": text[:500]}

def handle_tabular(context, path: str) -> dict:
    context.log.info(f"[Table] 读取表格文件: {path}")
    if path.endswith(".csv"):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)

    return {
        "type": "table",
        "path": path,
        "row_count": len(df),
        "columns": df.columns.tolist()
    }

# Step 4: 持久化结果（单独 op，需要声明资源依赖）
@op(ins={"data": In(dict)}, out=Out(io_manager_key="postgres_io_manager"), description="持久化结果")
def persist_result(context, data: dict):
    context.log.info("准备持久化结果")
    return data  # 返回会触发 IOManager 的 handle_output


# ---------- Step 4: 定义 Job ----------
@job
def document_recognition_pipeline():
    file_info = read_file()
    result = dispatch_process(file_info)
    persist_result(result)
from dagster import op, job, In, String,Out
from pdf2image import convert_from_path
import json
from synapse_flow.db import insert_job_detail,insert_pdf_info
from magic_pdf.data.data_reader_writer import FileBasedDataWriter, FileBasedDataReader
from magic_pdf.data.dataset import PymuDocDataset
from magic_pdf.model.doc_analyze_by_custom_model import doc_analyze
from magic_pdf.config.enums import SupportedPdfParseMethod
@op(ins={"pdf_path": In(String)},
    description="检查 PDF 文件大小是否超过 20MB")
@op(
    ins={"pdf_path": In(str)},
    description="检查 PDF 文件大小是否超过 20MB，并记录日志到 dataset_job_detail 表"
)
def check_pdf_size(context, pdf_path: str,task_id: int) -> str:
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    job_run_id = context.run_id
    job_name = context.op.name
    custom_id = os.path.basename(pdf_path)

    try:
        context.log.info(f"PDF 大小: {size_mb:.2f}MB")

        if size_mb > 500:
            raise Exception(f"PDF 文件过大：{size_mb:.2f}MB，不能超过 500MB，路径：{pdf_path}")

        # ✅ 正常情况下插入日志：状态 success
        insert_job_detail(job_run_id, job_name, custom_id,task_id)


        return pdf_path

    except Exception as e:
        # ❌ 异常情况下也插入日志（如果你扩展了日志表结构）
        context.log.error(str(e))
        # insert_job_detail(job_run_id, job_name, custom_id)  # 可扩展加上 status='fail', error_msg=str(e)
        raise

@op(ins={"pdf_path": In(String)},
    out=Out(dict),
    description="将 PDF 每一页转换为 PNG 图片并保存")
def process_pdf_file_to_pngs(context, pdf_path: str):
    context.log.info(f"开始处理 PDF 文件: {pdf_path}")
    images = convert_from_path(pdf_path, 300)
    output_dir = pdf_path.replace('.pdf', '_images')
    os.makedirs(output_dir, exist_ok=True)
    for i, image in enumerate(images):
        image.save(f"{output_dir}/page_{i + 1}.png", "PNG")
    context.log.info(f"PDF 转换完成，保存为 PNG 文件：{output_dir}")
    return {"code": "00000", "message": "success", "data": {"images_dir": output_dir}}

@op
def handle_result(context, result_from_prev: dict):
    context.log.info(f"收到处理结果：{result_from_prev}")
    images_dir = result_from_prev["data"]["images_dir"]


@op(ins={"pdf_path": In(String)},
    description="复制原始 PDF 文件（示例操作，可扩展为加水印等）")
def process_pdf_file_to_pdf(context, pdf_path: str):
    # 示例操作：复制一份 PDF，加水印或其他变换操作
    new_path = pdf_path.replace('.pdf', '_copy.pdf')
    with open(pdf_path, 'rb') as src, open(new_path, 'wb') as dst:
        dst.write(src.read())  # 简单复制，实际可以加水印等
    context.log.info(f"PDF 已复制到：{new_path}")


from dagster import op, In
import os


@op(
    ins={"pdf_path": In(str)},
    description="处理 PDF，输出图片、结构标注 PDF、Markdown 和 JSON 内容列表"
)
def process_pdf_file_to_markdown(context, pdf_path: str):
    context.log.info(f"开始结构化处理 PDF 文件: {pdf_path}")
    
    # 文件名与目录
    pdf_file_name = os.path.basename(pdf_path)
    name_without_suff = pdf_file_name.split(".")[0]
    local_image_dir, local_md_dir = "output/images", "output"
    os.makedirs(local_image_dir, exist_ok=True)
    
    image_writer = FileBasedDataWriter(local_image_dir)
    md_writer = FileBasedDataWriter(local_md_dir)
    image_dir = os.path.basename(local_image_dir)

    # 读取 PDF 为 bytes
    reader = FileBasedDataReader("")
    pdf_bytes = reader.read(pdf_path)

    # 构造数据集
    ds = PymuDocDataset(pdf_bytes)

    # 推理（OCR 或 非OCR）
    if ds.classify() == SupportedPdfParseMethod.OCR:
        context.log.info("文档类型为 OCR，使用 OCR 模式处理")
        infer_result = ds.apply(doc_analyze, ocr=True)
        pipe_result = infer_result.pipe_ocr_mode(image_writer)
    else:
        context.log.info("文档为文本型，使用文本模式处理")
        infer_result = ds.apply(doc_analyze, ocr=False)
        pipe_result = infer_result.pipe_txt_mode(image_writer)

    # 输出结果
    infer_result.draw_model(os.path.join(local_md_dir, f"{name_without_suff}_model.pdf"))
    pipe_result.draw_layout(os.path.join(local_md_dir, f"{name_without_suff}_layout.pdf"))
    pipe_result.draw_span(os.path.join(local_md_dir, f"{name_without_suff}_spans.pdf"))
    pipe_result.dump_md(md_writer, f"{name_without_suff}.md", image_dir)
    pipe_result.dump_content_list(md_writer, f"{name_without_suff}_content_list.json", image_dir)

    context.log.info(f"PDF 文件 {pdf_file_name} 处理完成，输出目录为 {local_md_dir}")


import os
import subprocess
from dagster import op, In, Out

import json
@op(ins={"pdf_path": In(str)}, out=Out(io_manager_key="postgres_io_manager"), description="提取 JSON 数据并保存至数据库")
def process_pdf_file_to_json(context, pdf_path: str):
    import os
    import subprocess
    import json

    try:
        run_id = context.run_id

        context.log.info(f"Run ID: {run_id}")

        output_dir = os.path.join("output_dir", str(run_id))
        os.makedirs(output_dir, exist_ok=True)
        context.log.info(f"输出目录为: {output_dir}")

        command = ["magic-pdf", "-p", pdf_path, "-o", output_dir, "-m", "auto"]
        context.log.info(f"Running command: {' '.join(command)}")

        result = subprocess.run(command, capture_output=True, text=True)
        context.log.info(f"magic-pdf stdout: {result.stdout}")
        context.log.info(f"magic-pdf stderr: {result.stderr}")
        context.log.info(f"magic-pdf return code: {result.returncode}")
        
        if result.returncode != 0:
            context.log.error(f"Error running magic-pdf: {result.stderr}")
            raise Exception(f"magic-pdf failed: {result.stderr}")
        context.log.info("magic-pdf 执行成功")

        pdf_filename = os.path.basename(pdf_path)
        pdf_name_without_ext = os.path.splitext(pdf_filename)[0]

        target_json = None
        layout_pdf_path = None

        # 列出输出目录中的所有文件
        context.log.info(f"输出目录 {output_dir} 中的所有文件:")
        for root, dirs, files in os.walk(output_dir):
            context.log.info(f"目录: {root}")
            context.log.info(f"子目录: {dirs}")
            context.log.info(f"文件: {files}")
            
            for file in files:
                if file.endswith("_content_list.json"):
                    target_json = os.path.join(root, file)
                    context.log.info(f"找到 JSON 文件: {target_json}")
                elif file.endswith("_layout.pdf"):
                    layout_pdf_path = os.path.join(root, file)
                    context.log.info(f"找到 _layout PDF 文件: {layout_pdf_path}")
                    insert_pdf_info(run_id, layout_pdf_path, pdf_name_without_ext)

        # 清理无关文件
        for root, dirs, files in os.walk(output_dir, topdown=False):
            for file in files:
                file_path = os.path.join(root, file)
                if file_path != target_json and file_path != layout_pdf_path:
                    os.remove(file_path)
                    context.log.info(f"删除文件: {file_path}")
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                try:
                    os.rmdir(dir_path)
                    context.log.info(f"删除空文件夹: {dir_path}")
                except OSError:
                    pass

        if not target_json or not os.path.exists(target_json):
            raise FileNotFoundError("未找到 _content_list.json 文件")

        with open(target_json, "r", encoding="utf-8") as f:
            json_data1 = json.load(f)

        context.log.info(f"读取的 JSON 数据: {json_data1}")

        # 处理并拼接 text 字段，带上 type
        content_list = []
        for item in json_data1:
            page = item.get("page_idx", item.get("page", -1))
            item_type = item.get("type", "unknown")

            if item_type == "text":
                text = item.get("text", "")
            elif item_type == "table":
                table_obj = {
                    "table_caption": item.get("table_caption", []),
                    "table_footnote": item.get("table_footnote", []),
                    "table_body": item.get("table_body", "")
                }
                text = json.dumps(table_obj, ensure_ascii=False)
            else:
                # 你可以根据需求处理其他类型，暂时跳过
                continue

            content_list.append({
                "page": page,
                "text": text,
                "type": item_type
            })

        json_data = {
            "file": pdf_path,
            "content": content_list
        }

        context.log.info(f"提取 JSON 数据: {json_data}")
        return json_data

    except subprocess.CalledProcessError as e:
        context.log.error(f"Subprocess error: {e}")
        raise

    except Exception as e:
        context.log.error(f"An error occurred: {e}")
        raise





@op(ins={"result_from_prev": In()}, description="处理 JSON 数据结果")
def handle_json(context, result_from_prev):
    context.log.info(f"从数据库读取的 JSON 数据: {result_from_prev}")



@job
def process_pdf_job():
    checked_path = check_pdf_size()
    pngs=process_pdf_file_to_pngs(checked_path)
    process_pdf_file_to_pdf(checked_path)
    process_pdf_file_to_markdown(checked_path)
    handle_result(pngs)
    jsonResult = process_pdf_file_to_json(checked_path)
    handle_json(jsonResult)

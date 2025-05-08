from dagster import op, job, In, String
from pdf2image import convert_from_path
import os

# 检查文件大小是否小于 200MB
@op(ins={"pdf_path": In(String)})
def check_pdf_size(context, pdf_path: str) -> str:
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    context.log.info(f"PDF 大小: {size_mb:.2f}MB")
    if size_mb > 20:
        raise Exception("PDF 文件过大，不能超过 20MB")
    return pdf_path  # 返回路径作为下一个 op 的输入


# 将 PDF 转图片
@op(ins={"pdf_path": In(String)})
def process_pdf_file(context, pdf_path: str):
    try:
        context.log.info(f"开始处理 PDF 文件: {pdf_path}")
        images = convert_from_path(pdf_path, 300)
        output_dir = pdf_path.replace('.pdf', '_images')
        os.makedirs(output_dir, exist_ok=True)
        for i, image in enumerate(images):
            image.save(f"{output_dir}/page_{i + 1}.png", "PNG")
        context.log.info(f"PDF 转换完成，保存为 PNG 文件：{output_dir}")
    except Exception as e:
        context.log.error(f"处理 PDF 文件时出错: {e}")
        raise

# 作业定义，按顺序执行
@job
def process_pdf_job():
    process_pdf_file(check_pdf_size())  # 使用上一步返回值

from dagster import op, job, In, String
from pdf2image import convert_from_path
import os

@op(ins={"pdf_path": In(String)},
    description="检查 PDF 文件大小是否超过 20MB")
def check_pdf_size(context, pdf_path: str) -> str:
    size_mb = os.path.getsize(pdf_path) / (1024 * 1024)
    context.log.info(f"PDF 大小: {size_mb:.2f}MB")
    if size_mb > 20:
        raise Exception(f"PDF 文件过大：{size_mb:.2f}MB，不能超过 20MB，文件路径：{pdf_path}")
    return pdf_path

@op(ins={"pdf_path": In(String)},
    description="将 PDF 每一页转换为 PNG 图片并保存")
def process_pdf_file_to_pngs(context, pdf_path: str):
    context.log.info(f"开始处理 PDF 文件: {pdf_path}")
    images = convert_from_path(pdf_path, 300)
    output_dir = pdf_path.replace('.pdf', '_images')
    os.makedirs(output_dir, exist_ok=True)
    for i, image in enumerate(images):
        image.save(f"{output_dir}/page_{i + 1}.png", "PNG")
    context.log.info(f"PDF 转换完成，保存为 PNG 文件：{output_dir}")

@op(ins={"pdf_path": In(String)},
    description="复制原始 PDF 文件（示例操作，可扩展为加水印等）")
def process_pdf_file_to_pdf(context, pdf_path: str):
    # 示例操作：复制一份 PDF，加水印或其他变换操作
    new_path = pdf_path.replace('.pdf', '_copy.pdf')
    with open(pdf_path, 'rb') as src, open(new_path, 'wb') as dst:
        dst.write(src.read())  # 简单复制，实际可以加水印等
    context.log.info(f"PDF 已复制到：{new_path}")

@job
def process_pdf_job():
    checked_path = check_pdf_size()
    process_pdf_file_to_pngs(checked_path)
    process_pdf_file_to_pdf(checked_path)

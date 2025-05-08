from dagster import op, job, In, String
from pdf2image import convert_from_path
import os

@op(ins={"pdf_path": In(String)})
def process_pdf_file(context, pdf_path: str):
    try:
        context.log.info(f"开始处理 PDF 文件: {pdf_path}")
        # 将 PDF 转换为图像
        images = convert_from_path(pdf_path, 300)
        
        # 设置输出目录
        output_dir = pdf_path.replace('.pdf', '_images')
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存每一页为 PNG 图片
        for i, image in enumerate(images):
            image.save(f"{output_dir}/page_{i + 1}.png", "PNG")
        
        context.log.info(f"PDF 转换完成，保存为 PNG 文件：{output_dir}")
    
    except Exception as e:
        # 捕获异常并记录错误信息
        context.log.error(f"处理 PDF 文件时出错: {e}")
        raise  # 重新抛出异常以便 Dagster 捕获并处理


@job
def process_pdf_job():
    process_pdf_file()

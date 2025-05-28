from dagster import asset
from pathlib import Path
import fitz  # PyMuPDF
from PIL import Image, ImageDraw

UPLOAD_PATH = Path("uploaded_files")
UPLOAD_PATH.mkdir(exist_ok=True)

PDF_FILE_PATH = UPLOAD_PATH / "uploaded.pdf"
OUTPUT_IMAGE_DIR = UPLOAD_PATH / "boxed_images"
OUTPUT_IMAGE_DIR.mkdir(exist_ok=True)

print("asset启动")

@asset
def render_pdf_pages_with_boxes() -> list:
    """
    �? PDF 每页转为图片，并在文本区域画红框，保存为 PNG�?
    输出每页图片的路径列表，供人工校对�?
    """
    if not PDF_FILE_PATH.exists():
        raise FileNotFoundError(f"找不到上传的 PDF 文件：{PDF_FILE_PATH}")

    doc = fitz.open(str(PDF_FILE_PATH))
    output_paths = []

    for i, page in enumerate(doc):
        # 获取文本块位�?
        blocks = page.get_text("blocks")

        # 渲染为像素图
        pix = page.get_pixmap(dpi=150)
        image_path = OUTPUT_IMAGE_DIR / f"page_{i+1}.png"
        pix.save(str(image_path))

        # 打开�? PIL 图像以画�?
        img = Image.open(str(image_path)).convert("RGB")
        draw = ImageDraw.Draw(img)
        for block in blocks:
            x0, y0, x1, y1 = block[:4]
            draw.rectangle([x0, y0, x1, y1], outline="red", width=2)

        img.save(str(image_path))
        output_paths.append(str(image_path))

    return output_paths

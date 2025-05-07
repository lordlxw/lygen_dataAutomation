import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify
from datetime import datetime
import logging

from dagster import DagsterInstance, build_reconstructable_job, execute_job,reconstructable
from DatasetAutomation_Dagster.jobs import render_pdf_job  # 确保导入正确

# 设置 DAGSTER_HOME 环境变量
os.environ["DAGSTER_HOME"] = str(Path.home() / "dagster_home")
Path(os.environ["DAGSTER_HOME"]).mkdir(exist_ok=True)

# 添加项目根目录到 sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
UPLOAD_PATH = Path("uploaded_files")
UPLOAD_PATH.mkdir(exist_ok=True)


@app.route('/upload', methods=['POST'])
def upload_pdf():
    try:
        if 'file' not in request.files:
            logger.error("No file part in the request")
            return "No file part", 400

        file = request.files['file']
        if file.filename == '':
            logger.error("No selected file")
            return "No selected file", 400

        # 获取上传文件的原始文件名
        original_filename = file.filename.rsplit('.', 1)[0]
        logger.info(f"上传了文件：{original_filename}")

        # 保存上传文件
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{original_filename}_{timestamp}.pdf"
        file_path = UPLOAD_PATH / filename
        file.save(file_path)
        logger.info(f"文件保存到路径：{file_path}")

        # 写入路径文件
        with open("current_pdf_path.txt", "w") as f:
            f.write(str(file_path.resolve()))

        # 使用 Dagster 实例
        logger.info("调用 Dagster 作业...")
        instance = DagsterInstance.ephemeral()  # 获取临时实例

        # 使用 reconstructable 来获取可重构的作业
        reconstructable_job = reconstructable(render_pdf_job)  # 直接传递 render_pdf_job

        # 执行 Dagster 作业
        result = execute_job(
            job=reconstructable_job,
            instance=instance,
            run_config={
                "ops": {
                    "render_pdf_pages_with_boxes": {
                        "config": {
                            "file_path": str(file_path.resolve())  # 传入文件路径
                        }
                    }
                }
            }
        )

        if not result.success:
            errors = [event.message for event in result.event_list if event.is_failure]
            logger.error(f"Dagster 执行失败：{errors}")
            return jsonify({
                "status": "Dagster job failed",
                "errors": errors
            }), 500

        logger.info("Dagster job 执行成功")
        return jsonify({
            "status": "success",
            "file": filename
        })

    except Exception as e:
        logger.exception("发生异常")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=6666)

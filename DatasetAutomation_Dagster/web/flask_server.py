import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify
from datetime import datetime
import logging

from dagster import DagsterInstance, in_process_executor, execute_job,reconstructable
from DatasetAutomation_Dagster.jobs import process_pdf_job  # 确保导入正确

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

# 创建一个全局的 Dagster 实例
dagster_instance = DagsterInstance.get()  # 使用全局持久化实例

# 最大文件大小限制（单位：字节）
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

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

        original_filename = file.filename.rsplit('.', 1)[0]
        logger.info(f"上传了文件：{original_filename}")

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{original_filename}_{timestamp}.pdf"
        file_path = UPLOAD_PATH / filename
        file.save(file_path)
        logger.info(f"文件保存到路径：{file_path}")

        with open("current_pdf_path.txt", "w") as f:
            f.write(str(file_path.resolve()))

        logger.info("调用 Dagster 作业...")
        reconstructable_job = reconstructable(process_pdf_job)

        result = execute_job(
            job=reconstructable_job,
            run_config={
                "ops": {
                    "check_pdf_size": {
                        "inputs": {
                            "pdf_path": str(file_path)
                        }
                    }
                },
                "execution": {
                    "config": {
                        "in_process": {}
                    }
                }
            },
            instance=dagster_instance
        )

        logger.info(vars(result))

        if result.success:
            return jsonify({
                "status": "Dagster job succeeded",
                "runId": result.run_id
            }), 200
        else:
            errors = [
                event.message
                for event in result.all_events
                if event.event_type_value == "STEP_FAILURE"
            ]
            return jsonify({
                "status": "Dagster job failed",
                "runId": None,
                "errors": errors
            }), 500

    except Exception as e:
        logger.exception("上传处理过程中发生异常")
        return jsonify({"status": "error", "message": str(e)}), 500








@app.route('/query_status', methods=['GET'])
def query_status():
    try:
        logger.info("query_status called")

        run_id = request.args.get('runId')
        if not run_id:
            logger.warning("runId is required but missing")
            return jsonify({"status": "error", "message": "runId is required"}), 400

        # 获取 DagsterRun 对象
        run = dagster_instance.get_run_by_id(run_id)
        print("runnnn")
        print(run)
        if not run:
            logger.error(f"Run with id {run_id} not found")
            return jsonify({"status": "error", "message": "Run not found"}), 404

        # 提取关键字段
        run_info = {
            "runId": run.run_id,
            "jobName": run.job_name,
            "runStatus": run.status.value,  # e.g. 'SUCCESS', 'FAILURE'
            "runConfig": run.run_config,  # 配置中包含了你传入的 pdf_path 等
        }

        # 提取执行路径、模块等运行元数据
        origin = run.job_code_origin
        if origin:
            run_info.update({
                "repositoryOrigin": str(origin.repository_origin) if origin.repository_origin else "N/A",
                "executablePath": origin.executable_path if origin.executable_path else "N/A"
            })

        return jsonify({
            "status": "success",
            "data": run_info
        }), 200

    except Exception as e:
        logger.exception("Error occurred while querying Dagster run status")
        return jsonify({"status": "error", "message": str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=6666)

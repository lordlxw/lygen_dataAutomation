import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify
from datetime import datetime
import logging
import threading
from dagster import DagsterInstance, in_process_executor, execute_job,reconstructable
from DatasetAutomation_Dagster.jobs import process_pdf_job  # 确保导入正确
from DatasetAutomation_Dagster.iomanagers import json_file_io_manager,sqlite_io_manager,postgres_io_manager
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
        # 校验文件是否存在
        if 'file' not in request.files:
            logger.error("No file part in the request")
            return jsonify({
                "message": "No file part in the request",
                "code": "00001",
                "value": None
            }), 400

        file = request.files['file']
        if file.filename == '':
            logger.error("No selected file")
            return jsonify({
                "message": "No selected file",
                "code": "00002",
                "value": None
            }), 400

        # 校验 route 参数
        route = request.form.get("route", "to_pngs")
        if route not in ["to_pngs", "to_pdf", "to_json"]:
            return jsonify({
                "message": "无效的 route 参数（应为 'to_pngs'、'to_pdf' 或 'to_json'）",
                "code": "00003",
                "value": None
            }), 400

        # 选择要执行的 op 分支
        if route == "to_pngs":
            op_selection = ["check_pdf_size", "process_pdf_file_to_pngs"]
        elif route == "to_pdf":
            op_selection = ["check_pdf_size", "process_pdf_file_to_pdf"]
        elif route == "to_json":
            op_selection = ["check_pdf_size", "process_pdf_file_to_json", "handle_json"]

        # 构造保存路径
        original_filename = file.filename.rsplit('.', 1)[0]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"{original_filename}_{timestamp}.pdf"
        file_path = UPLOAD_PATH / filename
        file.save(file_path)
        logger.info(f"文件保存到路径：{file_path}")

        # 记录当前 PDF 路径
        with open("current_pdf_path.txt", "w") as f:
            f.write(str(file_path.resolve()))

        logger.info("调用 Dagster 作业...")

        # 执行 Dagster 作业
        result = process_pdf_job.execute_in_process(
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
                },
                "resources": {
                    "postgres_io_manager": {
                        "config": {}
                    }
                }
            },
            resources={
                "postgres_io_manager": postgres_io_manager
            },
            op_selection=op_selection
        )

        # 输出作业事件日志
        for event in result.all_events:
            logger.info(f"{event.event_type_value}: {event.message}")

        if result.success:
            return jsonify({
                "message": "Dagster job succeeded",
                "code": "00000",
                "value": {
                    "runId": result.run_id
                }
            }), 200
        else:
            errors = [
                f"[{event.step_key}] {event.message}"
                for event in result.all_events
                if event.event_type_value == "STEP_FAILURE"
            ]
            return jsonify({
                "message": "Dagster job failed",
                "code": "00004",
                "value": {
                    "runId": None,
                    "errors": errors
                }
            }), 500

    except Exception as e:
        logger.exception("上传处理过程中发生异常")
        return jsonify({
            "message": str(e),
            "code": "00005",
            "value": None
        }), 500





@app.route('/query_status', methods=['GET'])
def query_status():
    try:
        logger.info("query_status called")

        run_id = request.args.get('runId')
        if not run_id:
            logger.warning("runId is required but missing")
            return jsonify({
                "message": "runId 参数缺失",
                "code": "00006",
                "value": None
            }), 400

        # 获取 DagsterRun 对象
        run = dagster_instance.get_run_by_id(run_id)
        if not run:
            logger.error(f"Run with id {run_id} not found")
            return jsonify({
                "message": f"未找到 ID 为 {run_id} 的运行任务",
                "code": "00007",
                "value": None
            }), 404

        # 提取关键字段
        run_info = {
            "runId": run.run_id,
            "jobName": run.job_name,
            "runStatus": run.status.value,  # 如 'SUCCESS', 'FAILURE'
            "stepKeysToExecute": run.step_keys_to_execute,
            "runConfig": run.run_config,
        }

        # 提取执行路径、模块等运行元数据
        origin = run.job_code_origin
        if origin:
            run_info.update({
                "repositoryOrigin": str(origin.repository_origin) if origin.repository_origin else "N/A",
                "executablePath": origin.executable_path if origin.executable_path else "N/A"
            })

        return jsonify({
            "message": "运行状态查询成功",
            "code": "00000",
            "value": run_info
        }), 200

    except Exception as e:
        logger.exception("查询 Dagster 运行状态时发生异常")
        return jsonify({
            "message": str(e),
            "code": "00008",
            "value": None
        }), 500





if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=6666)

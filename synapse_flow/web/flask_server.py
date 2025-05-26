import sys
import os
from pathlib import Path
from flask import Flask, request, jsonify,send_from_directory,send_file
from datetime import datetime
import logging
import threading
from dagster import DagsterInstance, in_process_executor, execute_job,reconstructable
from synapse_flow.db import get_pg_conn
from synapse_flow.jobs import process_pdf_job  # 确保导入正确
from synapse_flow.iomanagers import json_file_io_manager,sqlite_io_manager,postgres_io_manager

from synapse_flow.web.apis.dataset_task import dataset_task_bp  # 导入蓝图
from synapse_flow.web.apis.dataset_job import dataset_job_bp  # 导入蓝图
from synapse_flow.web.apis.prompt_job import prompt_job_bp  # 导入蓝图
from synapse_flow.web.apis.remote_file import remote_file_bp  # 导入蓝图
from synapse_flow.web.apis.document_recognition import document_recognition_bp  # 导入蓝图
# 设置 DAGSTER_HOME 环境变量
os.environ["DAGSTER_HOME"] = str(Path.home() / "dagster_home")
Path(os.environ["DAGSTER_HOME"]).mkdir(exist_ok=True)

# 添加项目根目录到 sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
from flasgger import Swagger



# 假设 "output_dir" 在当前目录下
app = Flask(__name__)
# 配置静态文件夹：将 output_dir 暴露在 /outputs 路径下
@app.route('/outputs/<path:filename>')
def serve_output_file(filename):
    output_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'output_dir'))
    full_path = os.path.join(output_root, filename)
    print(f"[DEBUG] 尝试访问文件: {full_path}")
    if not os.path.exists(full_path):
        print("[ERROR] 文件不存在！")
    return send_from_directory(output_root, filename)

@app.route("/download1")
def download():
    # return send_file('test.exe', as_attachment=True)
    print(f"[DEBUG] 尝试访问文件: 11112222")
    return send_file('2.jpg')



# 注册蓝图，设置 url_prefix 为 /api
app.register_blueprint(dataset_task_bp, url_prefix='/api')
app.register_blueprint(dataset_job_bp, url_prefix='/api')
app.register_blueprint(prompt_job_bp, url_prefix='/api')
app.register_blueprint(remote_file_bp, url_prefix='/api')
app.register_blueprint(document_recognition_bp, url_prefix='/api')
swagger = Swagger(app, template={
    "swagger": "2.0",
    "info": {
        "title": "SynapseFlow API",
        "description": "PDF 文件处理服务接口文档",
        "version": "1.0.0"
    },
    "host": "127.0.0.1:6666",
    "basePath": "/",
    "schemes": ["http"],
    "swagger_ui": "/swagger-ui"  # 如果这个路径是正确的
})


UPLOAD_PATH = Path("uploaded_files")
UPLOAD_PATH.mkdir(exist_ok=True)

# 创建一个全局的 Dagster 实例
dagster_instance = DagsterInstance.get()  # 使用全局持久化实例

# 最大文件大小限制（单位：字节）
app.config['MAX_CONTENT_LENGTH'] = 200 * 1024 * 1024  # 200MB

# @app.route('/upload', methods=['POST'])
# def upload_pdf():
#     """
#     上传 PDF 文件并触发处理流程（支持 task_id 参数）
#     ---
#     consumes:
#       - multipart/form-data
#     parameters:
#       - name: file
#         in: formData
#         type: file
#         required: true
#         description: 要上传的 PDF 文件
#       - name: route
#         in: formData
#         type: string
#         required: false
#         enum: ["to_pngs", "to_pdf", "to_json"]
#         default: "to_pngs"
#         description: 选择处理流程（默认是 to_pngs）
#       - name: task_id
#         in: formData
#         type: integer
#         required: true
#         description: 与任务绑定的 ID
#     responses:
#       200:
#         description: 成功执行 Dagster 作业
#       400:
#         description: 参数错误
#       500:
#         description: 服务器内部错误
#     """
#     try:
#         # 校验文件是否存在
#         if 'file' not in request.files:
#             logger.error("No file part in the request")
#             return jsonify({
#                 "message": "No file part in the request",
#                 "code": "00001",
#                 "value": None
#             }), 400

#         file = request.files['file']
#         if file.filename == '':
#             logger.error("No selected file")
#             return jsonify({
#                 "message": "No selected file",
#                 "code": "00002",
#                 "value": None
#             }), 400

#         # 校验 route 参数
#         route = request.form.get("route", "to_pngs")
#         if route not in ["to_pngs", "to_pdf", "to_json"]:
#             return jsonify({
#                 "message": "无效的 route 参数（应为 'to_pngs'、'to_pdf' 或 'to_json'）",
#                 "code": "00003",
#                 "value": None
#             }), 400

#         # 提取并校验 task_id
#         task_id = request.form.get("task_id")
#         if not task_id:
#             return jsonify({
#                 "message": "缺少 task_id 参数",
#                 "code": "00006",
#                 "value": None
#             }), 400

#         try:
#             task_id = int(task_id)
#         except ValueError:
#             return jsonify({
#                 "message": "task_id 必须是整数",
#                 "code": "00007",
#                 "value": None
#             }), 400

#         # 构造保存路径
#         original_filename = file.filename.rsplit('.', 1)[0]
#         timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
#         filename = f"{original_filename}_{timestamp}.pdf"
#         file_path = UPLOAD_PATH / filename
#         file.save(file_path)
#         logger.info(f"文件保存到路径：{file_path}")

#         # 写入当前 PDF 路径文件（可选）
#         with open("current_pdf_path.txt", "w") as f:
#             f.write(str(file_path.resolve()))

#         logger.info("调用 Dagster 作业...")

#         # 构建 op_selection
#         if route == "to_pngs":
#             op_selection = ["check_pdf_size", "process_pdf_file_to_pngs"]
#         elif route == "to_pdf":
#             op_selection = ["check_pdf_size", "process_pdf_file_to_pdf"]
#         elif route == "to_json":
#             op_selection = ["check_pdf_size", "process_pdf_file_to_json", "handle_json"]

#         # 执行 Dagster 作业
#         result = process_pdf_job.execute_in_process(
#             run_config={
#                 "ops": {
#                     "check_pdf_size": {
#                         "inputs": {
#                             "pdf_path": str(file_path),
#                             "task_id": task_id
#                         }
#                     }
#                 },
#                 "execution": {
#                     "config": {
#                         "in_process": {}
#                     }
#                 },
#                 "resources": {
#                     "postgres_io_manager": {
#                         "config": {}
#                     }
#                 }
#             },
#             resources={
#                 "postgres_io_manager": postgres_io_manager
#             },
#             op_selection=op_selection
#         )

#         # 输出事件日志
#         for event in result.all_events:
#             logger.info(f"{event.event_type_value}: {event.message}")

#         if result.success:
#             return jsonify({
#                 "message": "Dagster job succeeded",
#                 "code": "00000",
#                 "value": {
#                     "runId": result.run_id
#                 }
#             }), 200
#         else:
#             errors = [
#                 f"[{event.step_key}] {event.message}"
#                 for event in result.all_events
#                 if event.event_type_value == "STEP_FAILURE"
#             ]
#             return jsonify({
#                 "message": "Dagster job failed",
#                 "code": "00004",
#                 "value": {
#                     "runId": None,
#                     "errors": errors
#                 }
#             }), 500

#     except Exception as e:
#         logger.exception("上传处理过程中发生异常")
#         return jsonify({
#             "message": str(e),
#             "code": "00005",
#             "value": None
#         }), 500

@app.route('/upload', methods=['POST'])
def upload_pdf():
    try:
        file = request.files['file']
        route = request.form.get("route", "to_pngs")
        task_id = int(request.form.get("task_id"))

        filename = f"{Path(file.filename).stem}_{datetime.now().strftime('%Y%m%d%H%M%S')}.pdf"
        file_path = UPLOAD_PATH / filename
        file.save(file_path)

        op_selection = {
            "to_pngs": ["check_pdf_size", "process_pdf_file_to_pngs"],
            "to_pdf": ["check_pdf_size", "process_pdf_file_to_pdf"],
            "to_json": ["check_pdf_size", "process_pdf_file_to_json", "handle_json"]
        }.get(route)

        # ✅ 直接执行 Dagster 作业（同步）
        result = process_pdf_job.execute_in_process(
            run_config={
                "ops": {
                    "check_pdf_size": {
                        "inputs": {
                            "pdf_path": str(file_path),
                            "task_id": task_id
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

        logger.info(f"[Task {task_id}] Dagster run finished. Success: {result.success}, run_id: {result.run_id}")

        return jsonify({
            "message": "任务执行完成",
            "code": "00000" if result.success else "00002",
            "value": {
                "task_id": task_id,
                "run_id": result.run_id,
                "file": str(file_path.name)
            }
        }), 200

    except Exception as e:
        logger.exception("上传处理异常")
        return jsonify({
            "message": str(e),
            "code": "00005",
            "value": None
        }), 500





@app.route('/query_status', methods=['GET'])
def query_status():
    """
    查询 Dagster 任务执行状态
    ---
    parameters:
      - name: runId
        in: query
        type: string
        required: true
        description: Dagster 运行 ID
    responses:
      200:
        description: 返回任务状态信息
      404:
        description: 未找到指定任务
      500:
        description: 查询出错
    """
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


import psycopg2
from psycopg2.extras import RealDictCursor

# 假设你已有这个函数获取数据库连接
@app.route('/get_pdf_json', methods=['POST'])
def get_pdf_json():
    """
    获取 PDF 分析结果（从数据库查询）
    ---
    consumes:
      - application/json
    parameters:
      - in: body
        name: body
        required: true
        schema:
          type: object
          properties:
            runId:
              type: string
              example: "abc123"
    responses:
      200:
        description: 成功返回分析结果
      400:
        description: 缺少参数
      500:
        description: 查询出错
    """
    try:
        # 从请求的JSON体中获取runId
        data = request.get_json()
        run_id = data.get('runId')
        
        # 检查是否存在runId
        if not run_id:
            return jsonify({
                "message": "runId 参数缺失",
                "code": "00009",
                "value": None
            }), 400

        conn = get_pg_conn()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT run_id, text, text_level, type, page_index, create_time
                FROM pdf_json
                WHERE run_id = %s
                ORDER BY page_index
            """, (run_id,))
            rows = cur.fetchall()

        return jsonify({
            "message": "查询成功",
            "code": "00000",
            "value": rows
        }), 200

    except Exception as e:
        logger.exception("查询 pdf_json 时发生异常")
        return jsonify({
            "message": str(e),
            "code": "00010",
            "value": None
        }), 500

    finally:
        if 'conn' in locals():
            conn.close()



# if __name__ == '__main__':
#     app.run(debug=True, host='0.0.0.0', port=6666)
if __name__ == '__main__':
    print("[DEBUG] 当前所有路由：")
    for rule in app.url_map.iter_rules():
        print(rule)
        
    app.run(host='0.0.0.0', port=6666)


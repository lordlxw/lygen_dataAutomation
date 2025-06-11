from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.remote_file_service import getPdfByRunningId
from synapse_flow.web.utils.create_response import create_response
import os
from flask import Blueprint, request, send_from_directory, jsonify
# 定义蓝图
remote_file_bp = Blueprint('remote_file', __name__)

OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', 'output_dir'))


# 路由：获取pdf文件
@remote_file_bp.route('/getRemotePdf', methods=['POST'])
def getRemotePdf():
    data = request.get_json()
    runningId = data.get("run_id")
    print("调用getRemotePdf")

    if not runningId:
        return create_response(message="缺少参数 runningId", code="00001"), 400

    relative_path = getPdfByRunningId(runningId)
    if not relative_path:
        return create_response(message="未找到对应 PDF 路径", code="00002"), 404

    # 去掉多余的 output_dir
    if relative_path.lower().startswith("output_dir\\") or relative_path.lower().startswith("output_dir/"):
        relative_path = relative_path[len("output_dir/"):] if '/' in relative_path else relative_path[len("output_dir\\"):]

    full_path = os.path.join(OUTPUT_DIR, relative_path)

    print(f"[DEBUG] 尝试访问文件: {full_path}")
    if not os.path.exists(full_path):
        return create_response(message="PDF 文件不存在", code="00003"), 404

    directory = os.path.dirname(relative_path)
    filename = os.path.basename(relative_path)
    return send_from_directory(os.path.join(OUTPUT_DIR, directory), filename, as_attachment=True)

UPLOADS_DIR = "uploads"  # 你项目中上传文件的根目录
import os

@remote_file_bp.route('/getRemoteFile', methods=['POST'])
def getRemoteFile():
    data = request.get_json()
    runningId = data.get("run_id")
    file_type = data.get("type", "png")  # 默认png

    print("调用getRemoteFile")

    if not runningId:
        return create_response(message="缺少参数 runningId", code="00001"), 400

    folder_path = os.path.join("uploads", file_type, runningId)
    if not os.path.exists(folder_path):
        return create_response(message=f"未找到对应目录: {folder_path}", code="00002"), 404

    files = os.listdir(folder_path)
    if not files:
        return create_response(message=f"目录为空: {folder_path}", code="00003"), 404

    filename = files[0]

    abs_folder_path = os.path.abspath(folder_path)
    abs_file_path = os.path.join(abs_folder_path, filename)

    print(f"[DEBUG] 绝对目录: {abs_folder_path}")
    print(f"[DEBUG] 绝对文件路径: {abs_file_path}")

    if not os.path.exists(abs_file_path):
        return create_response(message="文件不存在", code="00004"), 404

    return send_from_directory(abs_folder_path, filename, as_attachment=True)






from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.pdf_operation_service import getAllPdfInfos,convert_pdf_to_images

# 定义蓝图
pdf_operation_bp = Blueprint('pdf_operation', __name__)

# 路由：获取所有任务
@pdf_operation_bp.route('/getAllPdfInfos', methods=['GET'])  # /task 路径
def get_tasks():
    pdf_infos = getAllPdfInfos()
    return create_response(data=pdf_infos, message="任务获取成功", code="00000")



# 路由：上传 PDF 并转换为图片
@pdf_operation_bp.route('/convertPdfToImages', methods=['POST'])
def convert_pdf_to_images_route():
    if 'file' not in request.files:
        return create_response(code="00001", message="未提供文件")

    file = request.files['file']
    if file.filename == '':
        return create_response(code="00002", message="文件名为空")

    result = convert_pdf_to_images(file.read())
    return create_response(
        code=result["code"],
        message=result["message"],
        data=result.get("value")
    )
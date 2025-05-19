from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.pdf_operation_service import getAllPdfInfos

# 定义蓝图
pdf_operation_bp = Blueprint('pdf_operation', __name__)

# 路由：获取所有任务
@pdf_operation_bp.route('/getAllPdfInfos', methods=['GET'])  # /task 路径
def get_tasks():
    pdf_infos = getAllPdfInfos()
    return create_response(data=pdf_infos, message="任务获取成功", code="00000")




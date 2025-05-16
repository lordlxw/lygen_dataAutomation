from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.dataset_job_service import insert_pdf_text_contents,query_pdf_text_contents

dataset_job_bp = Blueprint('dataset_job', __name__)

# 你已有的接口...

@dataset_job_bp.route('/insertVersionJson', methods=['POST'])
def insert_version_json():
    body = request.get_json()

    if not body or not isinstance(body, dict):
        return create_response(data=None, message="请求体必须是一个JSON对象", code="00001"), 400

    run_id = body.get("run_id")
    data_list = body.get("data_list")

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if not data_list or not isinstance(data_list, list):
        return create_response(data=None, message="缺少 data_list 或格式错误，必须是数组", code="00001"), 400

    try:
        insert_pdf_text_contents(run_id, data_list)
        return create_response(
            data={"run_id": run_id, "count": len(data_list)},
            message="批量插入成功",
            code="00000"
        )
    except Exception as e:
        return create_response(data=None, message=f"插入失败: {str(e)}", code="00002"), 500
    


from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.dataset_job_service import query_pdf_text_contents

dataset_job_bp = Blueprint('dataset_job', __name__)

@dataset_job_bp.route('/getPdfTextContents', methods=['POST'])
def get_pdf_text_contents():
    data = request.get_json()
    print('进入/getPdfTextContents')
    if not data:
        return create_response(data=None, message="缺少请求数据", code="00001"), 400

    run_id = data.get("run_id")
    version = data.get("version")

    if not run_id or not version:
        return create_response(data=None, message="缺少 run_id 或 version", code="00001"), 400

    try:
        results = query_pdf_text_contents(run_id, version)
        return create_response(data=results, message="查询成功", code="00000")
    except Exception as e:
        return create_response(data=None, message=f"查询失败: {str(e)}", code="00002"), 500





from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.dataset_job_service import insert_pdf_text_contents,query_pdf_text_contents,query_versions_by_run_id,query_all_pdf_infos,insert_change_log,query_change_log,query_based_version,query_pdf_infos_by_user_id

dataset_job_bp = Blueprint('dataset_job', __name__)

# 你已有的接口...

@dataset_job_bp.route('/insertVersionJson', methods=['POST'])
def insert_version_json():
    body = request.get_json()

    if not body or not isinstance(body, dict):
        return create_response(data=None, message="请求体必须是一个JSON对象", code="00001"), 400

    run_id = body.get("run_id")
    data_list = body.get("data_list")
    based_version = body.get("based_version")  # <-- 新增字段获取

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if not data_list or not isinstance(data_list, list):
        return create_response(data=None, message="缺少 data_list 或格式错误，必须是数组", code="00001"), 400

    try:
        print("insert_version_json")
        # 调用时传入 based_version，如果你在 insert_pdf_text_contents 函数中有用到就添加进去
        new_version = insert_pdf_text_contents(run_id, data_list, based_version=based_version)
        return create_response(
            data={"run_id": run_id, "count": len(data_list), "version": new_version},
            message="批量插入成功",
            code="00000"
        )
    except Exception as e:
        return create_response(data=None, message=f"插入失败: {str(e)}", code="00002"), 500


    




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


@dataset_job_bp.route('/getVersionList', methods=['POST'])
def get_version_list():
    data = request.get_json()

    if not data:
        return create_response(data=None, message="缺少请求数据", code="00001"), 400

    run_id = data.get("run_id")
    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400

    try:
        results = query_versions_by_run_id(run_id)
        return create_response(data=results, message="查询成功", code="00000")
    except Exception as e:
        return create_response(data=None, message=f"查询失败: {str(e)}", code="00002"), 500



@dataset_job_bp.route('/getAllPdfInfos', methods=['GET'])
def get_all_pdf_infos():
    """
    查询所有 PDF 信息，仅返回 original_pdf_name 和 create_time。
    """
    try:
        results = query_all_pdf_infos()
        return create_response(data=results, message="查询成功", code="00000")
    except Exception as e:
        return create_response(data=None, message=f"查询失败: {str(e)}", code="00002"), 500

import json


@dataset_job_bp.route('/getPdfInfosByUserId', methods=['POST'])
def get_pdf_infos_by_user_id():
    """
    根据 user_id 查询对应的 PDF 信息列表。
    """
    try:
        req_json = request.get_json()
        user_id = req_json.get('user_id')
        if not user_id:
            return create_response(data=None, message="缺少 user_id 参数", code="00002"), 400

        # 调用你实际查询方法，返回列表，比如字典列表
        results = query_pdf_infos_by_user_id(user_id)

        return create_response(data=results, message="查询成功", code="00000")
    except Exception as e:
        return create_response(data=None, message=f"查询失败: {str(e)}", code="00002"), 500


@dataset_job_bp.route('/insertChangeLog', methods=['POST'])
def insert_change_log_api():
    body = request.get_json()
    if not body or not isinstance(body, dict):
        return create_response(data=None, message="请求体必须是JSON对象", code="00001"), 400

    run_id = body.get("run_id")
    version = body.get("version")
    change_json_log = body.get("change_json_log")

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if version is None:
        return create_response(data=None, message="缺少 version", code="00001"), 400
    if not change_json_log:
        return create_response(data=None, message="缺少 change_json_log", code="00001"), 400

    # ✅ 序列化 change_json_log（防止 dict 类型无法插入 PG）
    try:
        change_json_log_str = json.dumps(change_json_log, ensure_ascii=False)
    except Exception as e:
        return create_response(data=None, message=f"change_json_log 解析失败: {str(e)}", code="00001"), 400

    try:
        success = insert_change_log(run_id, version, change_json_log_str)
        if success:
            return create_response(data=None, message="插入变更日志成功", code="00000")
        else:
            return create_response(data=None, message="插入变更日志失败", code="00002"), 500
    except Exception as e:
        return create_response(data=None, message=f"插入变更日志异常: {str(e)}", code="00002"), 500
    



@dataset_job_bp.route('/getChangeLog', methods=['POST'])
def get_change_log():
    data = request.get_json()
    if not data or not isinstance(data, dict):
        return create_response(data=None, message="请求体必须是JSON对象", code="00001"), 400

    run_id = data.get("run_id")
    version = data.get("version")

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if version is None:
        return create_response(data=None, message="缺少 version", code="00001"), 400

    try:
        logs = query_change_log(run_id, version)
        return create_response(data=logs, message="查询变更日志成功", code="00000")
    except Exception as e:
        return create_response(data=None, message=f"查询变更日志失败: {str(e)}", code="00002"), 500



@dataset_job_bp.route('/getBasedVersion', methods=['POST'])
def get_based_version():
    data = request.get_json()
    if not data or not isinstance(data, dict):
        return create_response(data=None, message="请求体必须是JSON对象", code="00001"), 400

    run_id = data.get("run_id")
    version = data.get("version")

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if version is None:
        return create_response(data=None, message="缺少 version", code="00001"), 400

    try:
        based_version = query_based_version(run_id, version)
        if based_version is not None:
            return create_response(data={"based_version": based_version}, message="查询成功", code="00000")
        else:
            return create_response(data=None, message="未找到对应版本", code="00002"), 404
    except Exception as e:
        return create_response(data=None, message=f"查询失败: {str(e)}", code="00002"), 500



# 根据runngingId更新userId
@dataset_job_bp.route('/updateUserIdByRunId', methods=['POST'])
def update_user_id_by_run_id_api():
    data = request.get_json()

    if not data or not isinstance(data, dict):
        return create_response(data=None, message="请求体必须是JSON对象", code="00001"), 400

    run_id = data.get("run_id")
    user_id = data.get("user_id")

    if not run_id:
        return create_response(data=None, message="缺少 run_id", code="00001"), 400
    if not user_id:
        return create_response(data=None, message="缺少 new_user_id", code="00001"), 400

    try:
        from synapse_flow.web.services.dataset_job_service import update_user_id_by_run_id
        success = update_user_id_by_run_id(run_id, user_id)
        if success:
            return create_response(data={"run_id": run_id, "new_user_id": user_id}, message="更新成功", code="00000")
        else:
            return create_response(data=None, message="更新失败", code="00002"), 500
    except Exception as e:
        return create_response(data=None, message=f"更新异常: {str(e)}", code="00002"), 500

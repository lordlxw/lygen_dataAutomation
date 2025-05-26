from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.document_recognition_service import save_upload_file, run_document_recognition,get_invoice_data_by_run_id

document_recognition_bp = Blueprint('document_recognition', __name__)

@document_recognition_bp.route('/uploadFile', methods=['POST'])
def upload_file():
    try:
        print("上传了！！！")
        file = request.files.get('file')
        if not file:
            return create_response(message="未上传文件", code="00001")

        # 1. 保存文件到临时目录
        file_path = save_upload_file(file)
        print("filepath",file_path)
        # return create_response(message=str(file_path), code="00002")
        # 2. 调用服务层执行Dagster及备份
        print("调用服务层执行Dagster及备份")
        result = run_document_recognition(file_path)
        print("result999",result)
        if not result["success"]:
            return create_response(message="任务执行失败", code="00002")

        return create_response(
            data={
                "run_id": result["run_id"],
                "backup_path": result["backup_path"],
                "original_file": file.filename
            },
            message="任务执行成功",
            code="00000"
        )
    except Exception as e:
        return create_response(message=f"异常: {str(e)}", code="00005")
    

# 新增接口，POST接收run_id，查询发票数据及明细
@document_recognition_bp.route('/getInvoiceData', methods=['POST'])
def get_invoice_data():
    try:
        data = request.get_json()
        run_id = data.get("run_id")
        if not run_id:
            return create_response(message="缺少参数 run_id", code="00001")
        
        invoice_data = get_invoice_data_by_run_id(run_id)
        if not invoice_data:
            return create_response(message="未找到对应发票数据", code="00002")
        
        return create_response(data=invoice_data, message="查询成功", code="00000")
    except Exception as e:
        return create_response(message=f"异常: {str(e)}", code="00005")

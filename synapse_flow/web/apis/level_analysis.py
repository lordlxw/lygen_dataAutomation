from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.level_analysis_service import update_pdf_json_hierarchy, analyze_hierarchy_by_run_id

# 定义蓝图
level_analysis_bp = Blueprint('level_analysis_bp', __name__)

@level_analysis_bp.route('/generateHierarchy', methods=['POST'])
def generate_hierarchy():
    """
    根据run_id生成层级分析
    接收run_id，自动从数据库查询数据并进行分析
    
    请求格式:
    {
        "run_id": "e46561b4-075c-47f8-80a2-efdeacb5cfa7"
    }
    
    返回格式:
    {
        "code": "00000",
        "message": "层级分析完成",
        "data": {
            "status": "success",
            "message": "成功更新 5 条记录"
        }
    }
    """
    try:
        # 获取请求数据
        request_data = request.get_json()
        
        if not request_data:
            return create_response(
                data=None,
                message="请求数据为空",
                code="40001"
            )
        
        # 获取run_id
        run_id = request_data.get('run_id')
        
        if not run_id:
            return create_response(
                data=None,
                message="缺少run_id参数",
                code="40002"
            )
        
        if not isinstance(run_id, str):
            return create_response(
                data=None,
                message="run_id参数应为字符串",
                code="40003"
            )
        
        print(f"开始处理run_id: {run_id} 的层级分析...")
        
        # 调用层级分析服务
        result = analyze_hierarchy_by_run_id(run_id)
        
        if result['status'] == 'success':
            # 只返回成功状态和简要信息
            return create_response(
                data={
                    "status": "success",
                    "message": result['message']
                },
                message="层级分析完成",
                code="00000"
            )
        else:
            # 返回失败状态和错误信息
            return create_response(
                data={
                    "status": "error",
                    "message": result['message']
                },
                message=result['message'],
                code="50001"
            )
        
    except Exception as e:
        print(f"层级分析接口异常: {str(e)}")
        return create_response(
            data={
                "status": "error",
                "message": f"层级分析异常: {str(e)}"
            },
            message=f"层级分析异常: {str(e)}",
            code="50000"
        )

@level_analysis_bp.route('/analyze_hierarchy', methods=['POST'])
def analyze_hierarchy():
    """
    层级分析接口（保持向后兼容）
    接收JSON数组，进行层级分析并更新数据库
    
    请求格式:
    {
        "data": [
            {
                "id": 123,
                "text": "第一章 房地产行业概况",
                "isTitleMarked": "section level"
            },
            {
                "id": 124,
                "text": "第一节 业务分类",
                "isTitleMarked": "section level"
            }
        ]
    }
    
    返回格式:
    {
        "code": "00000",
        "message": "层级分析完成",
        "data": {
            "status": "success",
            "message": "成功更新 2 条记录",
            "total_processed": 2,
            "updated_count": 2,
            "results": [...]
        }
    }
    """
    try:
        # 获取请求数据
        request_data = request.get_json()
        
        if not request_data:
            return create_response(
                data=None,
                message="请求数据为空",
                code="40001"
            )
        
        # 获取数据数组
        data_list = request_data.get('data', [])
        
        if not data_list:
            return create_response(
                data=None,
                message="数据数组为空",
                code="40002"
            )
        
        # 验证数据格式
        for i, item in enumerate(data_list):
            if not isinstance(item, dict):
                return create_response(
                    data=None,
                    message=f"第{i+1}项数据格式错误，应为对象",
                    code="40003"
                )
            
            # 检查必需字段
            required_fields = ['id', 'text', 'isTitleMarked']
            missing_fields = [field for field in required_fields if field not in item]
            
            if missing_fields:
                return create_response(
                    data=None,
                    message=f"第{i+1}项数据缺少必需字段: {', '.join(missing_fields)}",
                    code="40004"
                )
            
            # 验证字段类型
            if not isinstance(item['id'], int):
                return create_response(
                    data=None,
                    message=f"第{i+1}项数据的id字段应为整数",
                    code="40005"
                )
            
            if not isinstance(item['text'], str):
                return create_response(
                    data=None,
                    message=f"第{i+1}项数据的text字段应为字符串",
                    code="40006"
                )
            
            if not isinstance(item['isTitleMarked'], str):
                return create_response(
                    data=None,
                    message=f"第{i+1}项数据的isTitleMarked字段应为字符串",
                    code="40007"
                )
        
        print(f"开始处理 {len(data_list)} 条数据的层级分析...")
        
        # 调用层级分析服务
        result = update_pdf_json_hierarchy(data_list)
        
        if result['status'] == 'success':
            return create_response(
                data=result,
                message="层级分析完成",
                code="00000"
            )
        else:
            return create_response(
                data=result,
                message=result['message'],
                code="50001"
            )
        
    except Exception as e:
        print(f"层级分析接口异常: {str(e)}")
        return create_response(
            data=None,
            message=f"层级分析异常: {str(e)}",
            code="50000"
        )

@level_analysis_bp.route('/test_connection', methods=['GET'])
def test_connection():
    """
    测试vLLM服务连接
    """
    try:
        import requests
        
        # 测试vLLM服务连接
        response = requests.get("http://localhost:8201/health", timeout=10)
        
        if response.status_code == 200:
            return create_response(
                data={"status": "connected", "port": 8201},
                message="vLLM服务连接正常",
                code="00000"
            )
        else:
            return create_response(
                data={"status": "error", "port": 8201},
                message="vLLM服务连接异常",
                code="50001"
            )
            
    except Exception as e:
        return create_response(
            data={"status": "error", "port": 8201},
            message=f"vLLM服务连接失败: {str(e)}",
            code="50002"
        ) 
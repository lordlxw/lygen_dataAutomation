from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.prompt_job_service import split_text,get_api_key
from synapse_flow.promptJob import promptJobPipeLine  # 确保导入正确
# 定义蓝图
prompt_job_bp = Blueprint('prompt_job', __name__)

# 路由：获取所有任务
@prompt_job_bp.route('/textsplit', methods=['post'])  # /task 路径
def text_split():
    result = promptJobPipeLine.execute_in_process(
        run_config={
            "ops": {
                "read_excel_file": {  # ← 改成这里
                    "inputs": {
                        "file_path": "C:\\Users\\liu86\\Desktop\\涟元工作\\数据集自动化\\凯铭数据\\0514-问题回答数据集\\0514-问题回答数据集\\test-250514-V3.0.xlsx"
                    }
                }
            }
        }
    )

    # 获取运行结果
    output_data = result.output_for_node("read_excel_file")  # ← 这里也要改
    print("读取结果：", output_data)

    return "success"


@prompt_job_bp.route('/getApiKey', methods=['POST'])  # /task 路径
def get_latest_api_key():
    try:
        result = get_api_key()
        return create_response(data=result, message="apiKey获取成功", code="00000")
    except Exception as e:
        # 可以打印日志或者使用 logging 模块
        print(f"Error in get_latest_api_key: {e}")
        return create_response(data=None, message=f"获取apiKey失败：{str(e)}", code="99999")


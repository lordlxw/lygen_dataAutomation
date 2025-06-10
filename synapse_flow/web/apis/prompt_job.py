from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.prompt_job_service import split_text,get_api_key,process_qa_for_version_0
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




@prompt_job_bp.route('/processQA', methods=['POST'])
def process_qa():
    """
    对版本0的数据进行QA问答对处理，生成版本1
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
            run_id:
              type: string
              description: 要处理的run_id
              example: "abc123-def456-ghi789"
    responses:
      200:
        description: 成功执行QA问答对处理
      400:
        description: 参数错误
      500:
        description: 服务器内部错误
    """
    try:
        data = request.get_json()
        if not data:
            return create_response(data=None, message="缺少请求数据", code="00001"), 400
        
        run_id = data.get("run_id")
        if not run_id:
            return create_response(data=None, message="缺少run_id参数", code="00001"), 400
        
        print(f"开始处理QA问答对，run_id: {run_id}")
        
        # 调用服务层处理QA问答对
        result = process_qa_for_version_0(run_id)
        
        return create_response(
            data=result,
            message="QA问答对处理完成",
            code="00000"
        )
        
    except Exception as e:
        print(f"QA问答对处理失败: {str(e)}")
        return create_response(
            data=None,
            message=f"QA问答对处理失败: {str(e)}",
            code="00002"
        ), 500


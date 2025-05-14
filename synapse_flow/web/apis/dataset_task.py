from flask import Blueprint, request, jsonify
from synapse_flow.web.services.dataset_task_service import get_all_tasks, create_task

# 定义蓝图
dataset_task_bp = Blueprint('dataset_task', __name__)

# 路由：获取所有任务
@dataset_task_bp.route('/task', methods=['GET'])  # 修改为 /task
def get_tasks():
    # 调用服务层函数获取所有任务
    task_list = get_all_tasks()
    
    # 返回任务列表，格式化为 JSON
    return jsonify({
        "code": "00000",
        "message": "任务获取成功",
        "data": task_list
    })

# 路由：创建新任务
@dataset_task_bp.route('/task', methods=['POST'])  # 修改为 /task
def create_new_task():
    # 获取请求的 JSON 数据
    data = request.get_json()
    
    # 校验传入的数据
    if 'name' not in data:
        return jsonify({
            "code": "00001",
            "message": "任务名称不能为空",
            "data": None
        }), 400  # 返回 400 错误
    
    # 调用服务层函数创建任务
    new_task = create_task(data)
    
    # 返回新创建的任务数据
    return jsonify({
        "code": "00000",
        "message": "任务创建成功",
        "data": new_task
    })

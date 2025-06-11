from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.dataset_task_service import get_all_tasks, create_task

# 定义蓝图
dataset_task_bp = Blueprint('dataset_task', __name__)

# 路由：获取所有任务
@dataset_task_bp.route('/task', methods=['GET'])  # /task 路径
def get_tasks():
    task_list = get_all_tasks()
    return create_response(data=task_list, message="任务获取成功", code="00000")


# 路由：创建新任务
@dataset_task_bp.route('/task', methods=['POST'])  # /task 路径
def create_new_task():
    data = request.get_json()

    if 'name' not in data:
        return create_response(data=None, message="任务名称不能为空", code="00001"), 400

    new_task = create_task(data)
    return create_response(data=new_task, message="任务创建成功", code="00000")

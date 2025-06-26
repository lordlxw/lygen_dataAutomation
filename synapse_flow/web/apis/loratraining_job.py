from flask import Blueprint, request
import os
import tempfile
from werkzeug.utils import secure_filename
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.loratraining_job_service import train_lora_model, get_training_status, list_training_tasks

# 定义蓝图
loratraining_job_bp = Blueprint('loratraining_job_bp', __name__)

# 允许的文件扩展名
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    """检查文件扩展名是否允许"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# 进行微调训练
@loratraining_job_bp.route('/training', methods=['POST'])
def lora_training():
    """
    启动LoRA模型训练
    支持CSV文件上传和训练参数配置
    """
    try:
        # 检查是否有文件上传
        if 'file' not in request.files:
            return create_response(
                data=None,
                message="请上传CSV训练文件",
                code="40001"
            )
        
        file = request.files['file']
        if file.filename == '':
            return create_response(
                data=None,
                message="未选择文件",
                code="40002"
            )
        
        if not allowed_file(file.filename):
            return create_response(
                data=None,
                message="只支持CSV文件格式",
                code="40003"
            )
        
        # 获取用户ID（可以从session或token中获取）
        user_id = request.form.get('user_id', 'default_user')
        
        # 获取训练参数
        training_config = {
            "batch_size": int(request.form.get('batch_size', 2)),
            "num_epochs": int(request.form.get('num_epochs', 3)),
            "learning_rate": float(request.form.get('learning_rate', 1e-4))
        }
        
        # 保存上传的文件到临时目录
        temp_dir = tempfile.mkdtemp()
        filename = secure_filename(file.filename)
        csv_file_path = os.path.join(temp_dir, filename)
        file.save(csv_file_path)
        
        # 构建训练数据
        training_data = {
            "csv_file_path": csv_file_path,
            "original_filename": filename,
            "batch_size": training_config["batch_size"],
            "num_epochs": training_config["num_epochs"],
            "learning_rate": training_config["learning_rate"]
        }
        
        # 调用训练服务
        result = train_lora_model(training_data, user_id)
        
        if result.get('status') == 'error':
            return create_response(
                data=None,
                message=result.get('message', '训练启动失败'),
                code="50001"
            )
        
        return create_response(
            data=result,
            message="LoRA训练任务已启动",
            code="00000"
        )
        
    except Exception as e:
        return create_response(
            data=None,
            message=f"训练启动异常: {str(e)}",
            code="50000"
        )

@loratraining_job_bp.route('/status/<task_id>', methods=['GET'])
def get_training_task_status(task_id):
    """
    获取训练任务状态
    """
    try:
        result = get_training_status(task_id)
        
        if result.get('error'):
            return create_response(
                data=None,
                message=result.get('error'),
                code="40001"
            )
        
        return create_response(
            data=result,
            message="获取训练状态成功",
            code="00000"
        )
        
    except Exception as e:
        return create_response(
            data=None,
            message=f"获取训练状态异常: {str(e)}",
            code="50000"
        )

@loratraining_job_bp.route('/tasks', methods=['GET'])
def get_training_tasks():
    """
    获取训练任务列表
    """
    try:
        # 获取查询参数
        user_id = request.args.get('user_id')
        limit = request.args.get('limit', 20, type=int)
        
        # 限制最大数量
        if limit > 100:
            limit = 100
        
        result = list_training_tasks(user_id, limit)
        
        return create_response(
            data=result,
            message="获取训练任务列表成功",
            code="00000"
        )
        
    except Exception as e:
        return create_response(
            data=None,
            message=f"获取训练任务列表异常: {str(e)}",
            code="50000"
        )

@loratraining_job_bp.route('/upload_csv', methods=['POST'])
def upload_csv():
    """
    仅上传CSV文件，不启动训练
    用于测试文件上传功能
    """
    try:
        if 'file' not in request.files:
            return create_response(
                data=None,
                message="请上传CSV文件",
                code="40001"
            )
        
        file = request.files['file']
        if file.filename == '':
            return create_response(
                data=None,
                message="未选择文件",
                code="40002"
            )
        
        if not allowed_file(file.filename):
            return create_response(
                data=None,
                message="只支持CSV文件格式",
                code="40003"
            )
        
        # 保存文件到临时目录
        temp_dir = tempfile.mkdtemp()
        filename = secure_filename(file.filename)
        file_path = os.path.join(temp_dir, filename)
        file.save(file_path)
        
        # 验证CSV文件
        try:
            import pandas as pd
            df = pd.read_csv(file_path)
            required_columns = ["input", "output", "instruction"]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                return create_response(
                    data=None,
                    message=f"CSV文件缺少必需列: {', '.join(missing_columns)}",
                    code="40004"
                )
            
            # 过滤空行
            df = df[df['input'].notna() & df['input'].str.strip().ne('')]
            
            if len(df) == 0:
                return create_response(
                    data=None,
                    message="CSV文件中没有有效数据",
                    code="40005"
                )
            
            return create_response(
                data={
                    "filename": filename,
                    "file_path": file_path,
                    "total_rows": len(df),
                    "valid_rows": len(df)
                },
                message="CSV文件上传成功",
                code="00000"
            )
            
        except Exception as e:
            return create_response(
                data=None,
                message=f"CSV文件验证失败: {str(e)}",
                code="40006"
            )
        
    except Exception as e:
        return create_response(
            data=None,
            message=f"文件上传异常: {str(e)}",
            code="50000"
        )

from flask import Blueprint, request
from synapse_flow.web.utils.create_response import create_response
from synapse_flow.web.services.login_service import register_user, verify_login, update_password, delete_user

login_bp = Blueprint('login', __name__)

# 注册接口
@login_bp.route('/register', methods=['POST'])
def register():
    data = request.get_json()
    print("/register")
    print(data)
    username = data.get('username')
    password = data.get('password')
    nickname = data.get('nickname')

    if not username or not password or not nickname:
        return create_response(data=None, message="用户名、密码和昵称不能为空", code="00001"), 400

    result = register_user(username, password, nickname)
    if result['code'] == "00000":
        return create_response(data=None, message=result['message'], code="00000")
    else:
        return create_response(data=None, message=result['message'], code=result['code']), 400


# 登录接口
@login_bp.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username')
    password = data.get('password')

    if not username or not password:
        return create_response(data=None, message="用户名和密码不能为空", code="00001"), 400

    result = verify_login(username, password)
    if result['code'] == "00000":
        return create_response(data=result.get('data'), message=result['message'], code="00000")
    else:
        return create_response(data=None, message=result['message'], code=result['code']), 400

# 修改密码接口
@login_bp.route('/update_password', methods=['POST'])
def change_password():
    data = request.get_json()
    username = data.get('username')
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not username or not old_password or not new_password:
        return create_response(data=None, message="用户名、旧密码、新密码不能为空", code="00001"), 400

    result = update_password(username, old_password, new_password)
    if result['code'] == "00000":
        return create_response(data=None, message=result['message'], code="00000")
    else:
        return create_response(data=None, message=result['message'], code=result['code']), 400

# 删除用户接口
@login_bp.route('/delete_user', methods=['POST'])
def remove_user():
    data = request.get_json()
    username = data.get('username')

    if not username:
        return create_response(data=None, message="用户名不能为空", code="00001"), 400

    result = delete_user(username)
    if result['code'] == "00000":
        return create_response(data=None, message=result['message'], code="00000")
    else:
        return create_response(data=None, message=result['message'], code=result['code']), 400

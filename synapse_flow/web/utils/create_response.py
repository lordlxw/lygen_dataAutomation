from flask import jsonify

def create_response(data=None, message="Success", code="00000"):
    """
    统一响应格式函数
    """
    return jsonify({
        "message": message,
        "code": code,
        "value": data
    })

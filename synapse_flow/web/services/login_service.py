from synapse_flow.db import get_pg_conn
import bcrypt
import uuid
from datetime import datetime

def find_user_by_username(username):
    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        sql = "SELECT id, username, password_hash, nickname, created_at FROM users WHERE username = %s;"
        cursor.execute(sql, (username,))
        row = cursor.fetchone()
        if row:
            return {
                "id": row[0],
                "username": row[1],
                "password_hash": row[2],
                "nickname": row[3],  # 新增 nickname 字段
                "created_at": row[4].isoformat() if row[4] else None
            }
        return None
    except Exception as e:
        print(f"查询用户出错: {e}")
        return None
    finally:
        cursor.close()
        conn.close()


def register_user(username, password, nickname=None):
    if find_user_by_username(username):
        return {"code": "00001", "message": "用户名已存在"}

    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        user_id = str(uuid.uuid4())
        created_at = datetime.utcnow()

        sql = """
        INSERT INTO users (id, username, password_hash, nickname, created_at)
        VALUES (%s, %s, %s, %s, %s);
        """
        cursor.execute(sql, (user_id, username, password_hash, nickname, created_at))
        conn.commit()
        return {"code": "00000", "message": "注册成功"}
    except Exception as e:
        print(f"注册出错: {e}")
        return {"code": "99999", "message": "注册失败"}
    finally:
        cursor.close()
        conn.close()


def verify_login(username, password):
    user = find_user_by_username(username)
    if not user:
        return {"code": "00002", "message": "用户名不存在"}

    if not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        return {"code": "00003", "message": "密码错误"}

    return {
        "code": "00000",
        "message": "登录成功",
        "data": {
            "user_id": user["id"],
            "nickname": user["nickname"]
        }
    }


def update_password(username, old_password, new_password):
    user = find_user_by_username(username)
    if not user:
        return {"code": "00002", "message": "用户名不存在"}

    if not bcrypt.checkpw(old_password.encode(), user["password_hash"].encode()):
        return {"code": "00003", "message": "旧密码错误"}

    new_password_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()

    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        sql = "UPDATE users SET password_hash = %s WHERE username = %s;"
        cursor.execute(sql, (new_password_hash, username))
        conn.commit()
        return {"code": "00000", "message": "密码修改成功"}
    except Exception as e:
        print(f"修改密码出错: {e}")
        return {"code": "99999", "message": "密码修改失败"}
    finally:
        cursor.close()
        conn.close()

def delete_user(username):
    user = find_user_by_username(username)
    if not user:
        return {"code": "00002", "message": "用户名不存在"}

    conn = get_pg_conn()
    try:
        cursor = conn.cursor()
        sql = "DELETE FROM users WHERE username = %s;"
        cursor.execute(sql, (username,))
        conn.commit()
        return {"code": "00000", "message": "用户删除成功"}
    except Exception as e:
        print(f"删除用户出错: {e}")
        return {"code": "99999", "message": "用户删除失败"}
    finally:
        cursor.close()
        conn.close()

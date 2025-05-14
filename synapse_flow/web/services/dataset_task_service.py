# services/dataset_task_service.py
from synapse_flow.db import get_pg_conn

# 获取所有任务
def get_all_tasks():
    conn = get_pg_conn()
    try:
        # 创建一个游标对象，用于执行 SQL 查询
        cursor = conn.cursor()
        
        # 执行查询，获取所有任务
        cursor.execute("SELECT * FROM dataset_task")
        
        # 获取所有任务数据
        tasks = cursor.fetchall()
        
        # 将任务数据转换为字典形式
        task_list = [
            {
                'id': task[0],
                'task_name': task[1],
                'job_list': task[2],
                'current_job': task[3],
                'is_completed': task[4],
                'create_time': task[5]
            }
            for task in tasks
        ]
        
        return task_list
    finally:
        conn.close()

# 创建新任务
def create_task(data):
    conn = get_pg_conn()
    try:
        # 创建一个游标对象，用于执行 SQL 插入操作
        cursor = conn.cursor()
        
        # 执行插入任务 SQL
        cursor.execute("""
            INSERT INTO dataset_task (task_name, is_completed, create_time)
            VALUES (%s, %s, NOW()) RETURNING id
        """, (data['name'], False))
        
        # 获取新任务的 ID
        new_task_id = cursor.fetchone()[0]
        
        # 提交事务
        conn.commit()
        
        # 返回新创建的任务字典
        return {
            'id': new_task_id,
            'task_name': data['name'],
            'is_completed': False,
            'create_time': '刚创建'
        }
    finally:
        conn.close()

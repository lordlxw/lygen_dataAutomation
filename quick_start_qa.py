#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QA问答对处理功能快速启动脚本

本脚本帮助用户快速启动和测试QA问答对处理功能。
"""

import os
import sys
import subprocess
import time
import requests
import json

def check_flask_service():
    """检查Flask服务是否正在运行"""
    try:
        response = requests.get("http://localhost:6667/", timeout=5)
        return response.status_code == 200
    except:
        return False

def start_flask_service():
    """启动Flask服务"""
    print("🚀 启动Flask服务...")
    
    # 检查是否已经在运行
    if check_flask_service():
        print("✅ Flask服务已经在运行")
        return True
    
    try:
        # 启动Flask服务
        cmd = [sys.executable, "-m", "synapse_flow.web.flask_server"]
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        # 等待服务启动
        print("⏳ 等待服务启动...")
        for i in range(30):  # 最多等待30秒
            time.sleep(1)
            if check_flask_service():
                print("✅ Flask服务启动成功")
                return True
            print(f"  等待中... ({i+1}/30)")
        
        print("❌ 服务启动超时")
        return False
        
    except Exception as e:
        print(f"❌ 启动服务失败: {str(e)}")
        return False

def check_model_paths():
    """检查模型路径"""
    print("🔍 检查模型路径...")
    
    model_paths = [
        "/data/training/model/Meta-Llama-3.1-8B-Instruct",
        "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005"
    ]
    
    all_exist = True
    for path in model_paths:
        if os.path.exists(path):
            print(f"✅ {path}")
        else:
            print(f"❌ {path} (不存在)")
            all_exist = False
    
    return all_exist

def check_dependencies():
    """检查Python依赖"""
    print("📦 检查Python依赖...")
    
    required_packages = [
        "torch",
        "transformers", 
        "peft",
        "flask",
        "requests"
    ]
    
    all_installed = True
    for package in required_packages:
        try:
            __import__(package)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} (未安装)")
            all_installed = False
    
    return all_installed

def check_database_field():
    """检查数据库remark字段"""
    print("🗄️ 检查数据库字段...")
    
    try:
        from synapse_flow.db import get_pg_conn
        
        conn = get_pg_conn()
        with conn.cursor() as cur:
            # 检查remark字段是否存在
            cur.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'pdf_json' 
                AND column_name = 'remark'
            """)
            
            result = cur.fetchone()
            if result:
                print("✅ 数据库remark字段已存在")
                return True
            else:
                print("❌ 数据库缺少remark字段")
                print("请运行SQL脚本添加字段:")
                print("psql -d your_database -f add_remark_field.sql")
                return False
                
    except Exception as e:
        print(f"❌ 检查数据库字段失败: {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def test_qa_api():
    """测试QA API接口"""
    print("🧪 测试QA API接口...")
    
    # 使用测试run_id
    test_run_id = "test_qa_run_001"
    
    url = "http://localhost:6667/api/processQA"
    data = {"run_id": test_run_id}
    
    try:
        print(f"发送测试请求，run_id: {test_run_id}")
        response = requests.post(url, json=data, timeout=30)
        
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ API接口响应正常")
            print(f"响应内容: {json.dumps(result, ensure_ascii=False, indent=2)}")
            return True
        else:
            print(f"❌ API接口错误: {response.status_code}")
            print(f"响应内容: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print("❌ 无法连接到API接口")
        return False
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        return False

def main():
    """主函数"""
    print("=" * 60)
    print("QA问答对处理功能快速启动")
    print("=" * 60)
    
    # 1. 检查依赖
    print("\n1️⃣ 检查系统依赖...")
    deps_ok = check_dependencies()
    if not deps_ok:
        print("\n❌ 缺少必要的Python依赖包")
        print("请运行: pip install -r requirements_qa.txt")
        return
    
    # 2. 检查数据库字段
    print("\n2️⃣ 检查数据库字段...")
    db_ok = check_database_field()
    if not db_ok:
        print("\n❌ 数据库字段检查失败")
        return
    
    # 3. 检查模型路径
    print("\n3️⃣ 检查模型路径...")
    model_ok = check_model_paths()
    if not model_ok:
        print("\n⚠️ 模型路径不存在，请检查配置")
        print("请确保模型文件在正确的位置")
    
    # 4. 启动Flask服务
    print("\n4️⃣ 启动Flask服务...")
    service_ok = start_flask_service()
    if not service_ok:
        print("\n❌ Flask服务启动失败")
        return
    
    # 5. 测试API
    print("\n5️⃣ 测试API接口...")
    api_ok = test_qa_api()
    
    # 总结
    print("\n" + "=" * 60)
    print("启动结果总结:")
    print(f"✅ 依赖检查: {'通过' if deps_ok else '失败'}")
    print(f"✅ 数据库字段: {'通过' if db_ok else '失败'}")
    print(f"✅ 模型路径: {'通过' if model_ok else '警告'}")
    print(f"✅ 服务启动: {'通过' if service_ok else '失败'}")
    print(f"✅ API测试: {'通过' if api_ok else '失败'}")
    
    if deps_ok and db_ok and service_ok:
        print("\n🎉 系统启动成功!")
        print("\n📋 使用说明:")
        print("1. 服务地址: http://localhost:6667")
        print("2. API接口: POST /api/processQA")
        print("3. 测试脚本: python test_qa_simple.py <run_id>")
        print("4. 查看文档: QA_USAGE.md")
        print("5. AI分析结果存储在数据库remark字段中")
    else:
        print("\n❌ 系统启动失败，请检查错误信息")
    
    print("=" * 60)

if __name__ == "__main__":
    main() 
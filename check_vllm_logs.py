#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import subprocess
import requests

def check_vllm_container():
    """检查vLLM容器状态和日志"""
    print("=== 检查vLLM容器状态 ===")
    
    # 检查容器状态
    try:
        result = subprocess.run(["docker", "ps", "-a", "--filter", "name=vllm"], 
                              capture_output=True, text=True)
        print("容器状态:")
        print(result.stdout)
    except Exception as e:
        print(f"检查容器状态失败: {e}")
    
    # 检查容器日志
    print("\n=== 检查vLLM容器日志 ===")
    try:
        result = subprocess.run(["docker", "logs", "--tail", "50", "vllm"], 
                              capture_output=True, text=True)
        print("容器日志:")
        print(result.stdout)
        
        if result.stderr:
            print("错误日志:")
            print(result.stderr)
    except Exception as e:
        print(f"检查容器日志失败: {e}")
    
    # 检查服务健康状态
    print("\n=== 检查服务健康状态 ===")
    try:
        response = requests.get("http://localhost:8201/health", timeout=10)
        print(f"健康检查状态码: {response.status_code}")
        if response.status_code == 200:
            print("✅ 服务健康")
        else:
            print(f"❌ 服务不健康: {response.text}")
    except Exception as e:
        print(f"❌ 健康检查失败: {e}")
    
    # 检查模型列表
    print("\n=== 检查可用模型 ===")
    try:
        response = requests.get("http://localhost:8201/v1/models", timeout=10)
        print(f"模型列表API状态码: {response.status_code}")
        if response.status_code == 200:
            models = response.json()
            print("可用模型:")
            for model in models.get("data", []):
                print(f"  - {model.get('id', 'unknown')}")
        else:
            print(f"❌ 获取模型列表失败: {response.text}")
    except Exception as e:
        print(f"❌ 模型列表检查失败: {e}")

if __name__ == "__main__":
    check_vllm_container() 
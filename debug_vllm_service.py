#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json

def check_vllm_service():
    """检查vLLM服务状态"""
    base_url = "http://localhost:8201"
    
    print("=== vLLM服务调试信息 ===")
    
    # 1. 检查健康状态
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        print(f"健康检查: {response.status_code}")
        if response.status_code == 200:
            print("✅ 服务健康")
        else:
            print(f"❌ 服务不健康: {response.text}")
    except Exception as e:
        print(f"❌ 健康检查失败: {e}")
    
    # 2. 检查模型列表
    try:
        response = requests.get(f"{base_url}/v1/models", timeout=10)
        print(f"\n模型列表API: {response.status_code}")
        if response.status_code == 200:
            models = response.json()
            print("可用模型:")
            for model in models.get("data", []):
                print(f"  - {model.get('id', 'unknown')}")
        else:
            print(f"❌ 获取模型列表失败: {response.text}")
    except Exception as e:
        print(f"❌ 模型列表检查失败: {e}")
    
    # 3. 测试简单的API调用
    try:
        url = f"{base_url}/v1/chat/completions"
        payload = {
            "model": "llama3.1_8b",
            "messages": [{"role": "user", "content": "你好"}],
            "max_tokens": 10,
            "temperature": 0.0
        }
        
        print(f"\n测试API调用: {url}")
        print(f"请求payload: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        response = requests.post(url, json=payload, timeout=30)
        print(f"响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("✅ API调用成功")
            print(f"响应: {json.dumps(result, ensure_ascii=False, indent=2)}")
        else:
            print(f"❌ API调用失败")
            print(f"响应内容: {response.text}")
            
    except Exception as e:
        print(f"❌ API调用测试失败: {e}")
    
    # 4. 尝试不同的模型名称
    print("\n=== 尝试不同的模型名称 ===")
    model_names = ["llama3.1_8b", "llama3.1-8b", "llama3.1_8b_instruct", "meta-llama-3.1-8b-instruct"]
    
    for model_name in model_names:
        try:
            payload = {
                "model": model_name,
                "messages": [{"role": "user", "content": "你好"}],
                "max_tokens": 10,
                "temperature": 0.0
            }
            
            response = requests.post(f"{base_url}/v1/chat/completions", json=payload, timeout=10)
            print(f"模型 '{model_name}': {response.status_code}")
            
            if response.status_code == 200:
                print(f"✅ 模型 '{model_name}' 可用")
                break
            else:
                print(f"❌ 模型 '{model_name}' 不可用: {response.text[:100]}")
                
        except Exception as e:
            print(f"❌ 模型 '{model_name}' 测试失败: {e}")

if __name__ == "__main__":
    check_vllm_service() 
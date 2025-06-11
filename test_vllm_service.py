#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import time

def test_vllm_service():
    """测试vLLM服务"""
    url = "http://localhost:8201/v1/chat/completions"
    
    # 简单的测试消息
    messages = [
        {"role": "user", "content": "你好，请简单介绍一下自己。"}
    ]
    
    payload = {
        "model": "llama3.1_8b",
        "messages": messages,
        "max_tokens": 100,
        "temperature": 0.0,
        "stream": False
    }
    
    try:
        print("测试vLLM服务...")
        response = requests.post(url, json=payload, timeout=60)
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            print(f"✅ 服务正常，响应: {content[:50]}...")
            return True
        else:
            print(f"❌ 服务异常，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 服务测试失败: {str(e)}")
        return False

def main():
    """主函数"""
    print("开始测试vLLM服务...")
    
    # 测试健康检查
    try:
        response = requests.get("http://localhost:8201/health", timeout=10)
        if response.status_code == 200:
            print("✅ 健康检查通过")
        else:
            print("❌ 健康检查失败")
    except Exception as e:
        print(f"❌ 健康检查异常: {str(e)}")
    
    print("\n开始API测试...")
    
    # 测试API调用
    if test_vllm_service():
        print("🎉 vLLM服务运行正常！")
    else:
        print("⚠️ vLLM服务可能有问题，请检查日志")

if __name__ == "__main__":
    main() 
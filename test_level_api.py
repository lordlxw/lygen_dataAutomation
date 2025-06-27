#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试层级分析API
"""

import requests
import json

def test_level_api():
    """测试层级分析API"""
    
    # 测试服务状态
    try:
        resp = requests.get("http://localhost:8202/v1/models", timeout=10)
        print(f"服务状态检查: {resp.status_code}")
        if resp.status_code == 200:
            models = resp.json()
            print(f"可用模型: {[model['id'] for model in models['data']]}")
        else:
            print("服务不可用")
            return
    except Exception as e:
        print(f"服务连接失败: {e}")
        return
    
    # 测试API调用
    url = "http://localhost:8202/v1/chat/completions"
    payload = {
        "model": "llama3.1_8b",
        "messages": [
            {"role": "system", "content": "你是一个文本层级梳理专家。"},
            {"role": "user", "content": "请问：段落层级：增值税纳税人与扣缴义务人有着不同的基本定义与性质（context level）是第几层级的开头内容？"}
        ],
        "max_tokens": 2000,
        "temperature": 0.0,
        "stream": False
    }
    
    try:
        print("\n开始API调用测试...")
        response = requests.post(url, json=payload, timeout=300)
        print(f"API响应状态码: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            content = result["choices"][0]["message"]["content"]
            print(f"API调用成功！")
            print(f"响应内容: {content}")
        else:
            print(f"API调用失败: {response.text}")
            
    except Exception as e:
        print(f"API调用出错: {e}")

if __name__ == "__main__":
    test_level_api() 
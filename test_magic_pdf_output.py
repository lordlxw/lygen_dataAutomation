#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试magic-pdf命令的输出结构
"""

import subprocess
import os
import json

def test_magic_pdf_output():
    """测试magic-pdf命令的输出"""
    
    print("=== 测试magic-pdf输出结构 ===")
    
    # 1. 检查是否有PDF文件
    pdf_files = [f for f in os.listdir(".") if f.endswith(".pdf")]
    if not pdf_files:
        print("当前目录没有PDF文件")
        return False
    
    pdf_file = pdf_files[0]
    print(f"使用PDF文件: {pdf_file}")
    
    # 2. 创建输出目录
    output_dir = "test_output"
    os.makedirs(output_dir, exist_ok=True)
    print(f"输出目录: {output_dir}")
    
    # 3. 运行magic-pdf命令
    command = ["magic-pdf", "-p", pdf_file, "-o", output_dir, "-m", "auto"]
    print(f"执行命令: {' '.join(command)}")
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        print(f"返回码: {result.returncode}")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")
        
        if result.returncode != 0:
            print("magic-pdf执行失败")
            return False
        
        print("magic-pdf执行成功")
        
        # 4. 检查输出文件
        print(f"\n=== 输出目录 {output_dir} 的内容 ===")
        for root, dirs, files in os.walk(output_dir):
            print(f"目录: {root}")
            print(f"子目录: {dirs}")
            print(f"文件: {files}")
            
            for file in files:
                file_path = os.path.join(root, file)
                print(f"  - {file_path}")
                
                # 检查是否是JSON文件
                if file.endswith(".json"):
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                            print(f"    JSON文件，包含 {len(data)} 个元素")
                            if data:
                                print(f"    第一个元素: {data[0]}")
                    except Exception as e:
                        print(f"    读取JSON文件失败: {e}")
        
        return True
        
    except subprocess.TimeoutExpired:
        print("magic-pdf执行超时")
        return False
    except Exception as e:
        print(f"执行出错: {e}")
        return False

if __name__ == "__main__":
    test_magic_pdf_output() 
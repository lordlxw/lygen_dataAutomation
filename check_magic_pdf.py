#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查magic-pdf命令
"""

import subprocess
import os

def check_magic_pdf():
    """检查magic-pdf命令"""
    
    print("=== 检查magic-pdf命令 ===")
    
    # 1. 检查magic-pdf是否安装
    try:
        result = subprocess.run(["magic-pdf", "--help"], capture_output=True, text=True)
        print(f"magic-pdf --help 返回码: {result.returncode}")
        if result.returncode == 0:
            print("✅ magic-pdf 命令可用")
            print(f"帮助信息: {result.stdout[:200]}...")
        else:
            print("❌ magic-pdf 命令不可用")
            print(f"错误信息: {result.stderr}")
            return False
    except FileNotFoundError:
        print("❌ magic-pdf 命令未找到，请检查是否已安装")
        return False
    
    # 2. 检查当前目录
    print(f"\n当前工作目录: {os.getcwd()}")
    
    # 3. 检查是否有PDF文件
    pdf_files = [f for f in os.listdir(".") if f.endswith(".pdf")]
    if pdf_files:
        print(f"找到PDF文件: {pdf_files}")
    else:
        print("未找到PDF文件")
    
    return True

if __name__ == "__main__":
    check_magic_pdf() 
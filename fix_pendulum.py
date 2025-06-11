#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复pendulum兼容性问题的补丁脚本
"""

import os
import sys

def fix_pendulum_compatibility():
    """修复pendulum兼容性问题"""
    
    # 找到dagster的pendulum兼容性文件
    try:
        import dagster
        dagster_path = os.path.dirname(dagster.__file__)
        pendulum_compat_file = os.path.join(dagster_path, "_seven", "compat", "pendulum.py")
        
        if os.path.exists(pendulum_compat_file):
            print(f"找到文件: {pendulum_compat_file}")
            
            # 读取原文件
            with open(pendulum_compat_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 备份原文件
            backup_file = pendulum_compat_file + '.backup'
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"已备份原文件到: {backup_file}")
            
            # 修复兼容性问题
            # 将 pendulum.Pendulum 替换为 pendulum.DateTime
            fixed_content = content.replace(
                'pendulum.DateTime if _IS_PENDULUM_2 else pendulum.Pendulum',
                'pendulum.DateTime'
            )
            
            # 写回修复后的内容
            with open(pendulum_compat_file, 'w', encoding='utf-8') as f:
                f.write(fixed_content)
            
            print("✅ 已修复pendulum兼容性问题")
            return True
        else:
            print(f"❌ 未找到文件: {pendulum_compat_file}")
            return False
            
    except Exception as e:
        print(f"❌ 修复失败: {str(e)}")
        return False

def restore_backup():
    """恢复备份文件"""
    try:
        import dagster
        dagster_path = os.path.dirname(dagster.__file__)
        pendulum_compat_file = os.path.join(dagster_path, "_seven", "compat", "pendulum.py")
        backup_file = pendulum_compat_file + '.backup'
        
        if os.path.exists(backup_file):
            with open(backup_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            with open(pendulum_compat_file, 'w', encoding='utf-8') as f:
                f.write(content)
            
            print("✅ 已恢复备份文件")
            return True
        else:
            print("❌ 未找到备份文件")
            return False
            
    except Exception as e:
        print(f"❌ 恢复失败: {str(e)}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "restore":
        restore_backup()
    else:
        fix_pendulum_compatibility() 
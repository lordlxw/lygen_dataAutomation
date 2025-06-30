#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
模型配置文件
支持多个微调模型的配置和切换
"""

import os
from typing import Dict, Any, Optional

# 模型配置字典
MODEL_CONFIGS = {
    "qa_model": {
        "name": "qa_model",
        "display_name": "QA问答模型",
        "base_model_path": "/data/training/model/Meta-Llama-3.1-8B-Instruct",
        "lora_path": "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005",
        "lora_module_name": "llama3.1_8b",
        "port": 8201,
        "container_name": "vllm_qa",
        "description": "用于QA问答对处理的微调模型"
    },
    "level_model": {
        "name": "level_model", 
        "display_name": "层级分析模型",
        "base_model_path": "/data/training/model/Meta-Llama-3.1-8B-Instruct",
        "lora_path": "/home/liuxinwei/Models/层级训练",  # 你的新模型路径
        "lora_module_name": "llama3.1_8b",
        "port": 8202,
        "container_name": "vllm_level",
        "description": "用于层级分析的微调模型"
    },
    "default_model": {
        "name": "default_model",
        "display_name": "默认模型",
        "base_model_path": "/data/training/model/Meta-Llama-3.1-8B-Instruct",
        "lora_path": "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005",
        "lora_module_name": "llama3.1_8b",
        "port": 8201,
        "container_name": "vllm_default",
        "description": "默认微调模型"
    }
}

class ModelManager:
    """模型管理器"""
    
    def __init__(self):
        self.current_model = "default_model"
        self.active_models = {}  # 存储已启动的模型服务
    
    def get_model_config(self, model_name: str) -> Optional[Dict[str, Any]]:
        """获取指定模型的配置"""
        return MODEL_CONFIGS.get(model_name)
    
    def get_current_model_config(self) -> Dict[str, Any]:
        """获取当前模型的配置"""
        return MODEL_CONFIGS.get(self.current_model, MODEL_CONFIGS["default_model"])
    
    def set_current_model(self, model_name: str) -> bool:
        """设置当前使用的模型"""
        if model_name in MODEL_CONFIGS:
            self.current_model = model_name
            return True
        return False
    
    def list_available_models(self) -> Dict[str, Dict[str, Any]]:
        """列出所有可用的模型"""
        return MODEL_CONFIGS.copy()
    
    def is_model_running(self, model_name: str) -> bool:
        """检查指定模型是否正在运行"""
        return model_name in self.active_models
    
    def register_active_model(self, model_name: str, port: int):
        """注册已启动的模型"""
        self.active_models[model_name] = {
            "port": port,
            "status": "running"
        }
    
    def unregister_active_model(self, model_name: str):
        """注销已停止的模型"""
        if model_name in self.active_models:
            del self.active_models[model_name]

# 全局模型管理器实例
model_manager = ModelManager()

def get_model_config(model_name: str) -> Optional[Dict[str, Any]]:
    """获取模型配置的便捷函数"""
    return model_manager.get_model_config(model_name)

def get_current_model_config() -> Dict[str, Any]:
    """获取当前模型配置的便捷函数"""
    return model_manager.get_current_model_config()

def set_current_model(model_name: str) -> bool:
    """设置当前模型的便捷函数"""
    return model_manager.set_current_model(model_name) 
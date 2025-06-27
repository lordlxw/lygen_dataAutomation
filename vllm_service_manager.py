#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vLLM服务管理器
支持多模型启动、管理和切换
"""
import os
import subprocess
import time
import requests
import json
import logging
from typing import Dict, Any, Optional, List
from model_config import ModelManager, get_model_config

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class VLLMServiceManager:
    """vLLM服务管理器"""
    
    def __init__(self):
        self.model_manager = ModelManager()
        self.active_services = {}  # 存储活跃的服务信息
    
    def start_model_service(self, model_name: str, force_restart: bool = False) -> Dict[str, Any]:
        """
        启动指定模型的服务
        
        Args:
            model_name: 模型名称
            force_restart: 是否强制重启
            
        Returns:
            Dict: 启动结果
        """
        try:
            # 获取模型配置
            model_config = get_model_config(model_name)
            if not model_config:
                return {
                    "success": False,
                    "message": f"模型配置不存在: {model_name}",
                    "model_name": model_name
                }
            
            # 检查服务是否已经运行
            if not force_restart and self.is_service_running(model_config["port"]):
                logger.info(f"模型 {model_name} 服务已在端口 {model_config['port']} 运行")
                self.active_services[model_name] = {
                    "port": model_config["port"],
                    "status": "running",
                    "config": model_config
                }
                return {
                    "success": True,
                    "message": f"模型 {model_name} 服务已在运行",
                    "model_name": model_name,
                    "port": model_config["port"]
                }
            
            # 停止可能存在的旧服务
            self.stop_model_service(model_name)
            
            # 启动新服务
            logger.info(f"启动模型 {model_name} 服务，端口: {model_config['port']}")
            
            # 构建docker命令
            cmd = self._build_docker_command(model_config)
            
            # 执行启动命令
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0:
                logger.info(f"✅ 模型 {model_name} 服务启动成功，端口: {model_config['port']}")
                
                # 等待服务启动
                if self._wait_for_service_ready(model_config["port"]):
                    self.active_services[model_name] = {
                        "port": model_config["port"],
                        "status": "running",
                        "config": model_config
                    }
                    return {
                        "success": True,
                        "message": f"模型 {model_name} 服务启动成功",
                        "model_name": model_name,
                        "port": model_config["port"]
                    }
                else:
                    return {
                        "success": False,
                        "message": f"模型 {model_name} 服务启动超时",
                        "model_name": model_name,
                        "port": model_config["port"]
                    }
            else:
                error_msg = result.stderr.strip()
                logger.error(f"❌ 模型 {model_name} 服务启动失败: {error_msg}")
                return {
                    "success": False,
                    "message": f"模型 {model_name} 服务启动失败: {error_msg}",
                    "model_name": model_name,
                    "port": model_config["port"]
                }
                
        except Exception as e:
            logger.error(f"启动模型 {model_name} 服务时出错: {str(e)}")
            return {
                "success": False,
                "message": f"启动模型 {model_name} 服务时出错: {str(e)}",
                "model_name": model_name
            }
    
    def _build_docker_command(self, model_config: Dict[str, Any]) -> List[str]:
        """构建docker启动命令"""
        cmd = [
            "docker", "run",
            "--gpus", "all",
            "-v", "/data/.cache/vllm:/root/.cache/vllm",
            "-v", "/data/.cache/huggingface:/root/.cache/huggingface",
            "-v", "/data/training/model:/root/model",
            "-v", f"{os.path.dirname(model_config['lora_path'])}:/root/lora",
            f"-p", f"{model_config['port']}:8000",
            "--ipc=host",
            "-d",
            f"--name", model_config["container_name"],
            "vllm/vllm-openai:latest",
            "--enable-lora",
            f"--lora-modules", f"{model_config['lora_module_name']}=/root/lora/{os.path.basename(model_config['lora_path'])}",
            "--model", model_config["base_model_path"],
            "--tensor-parallel-size", "8"
        ]
        return cmd
    
    def stop_model_service(self, model_name: str) -> Dict[str, Any]:
        """
        停止指定模型的服务
        
        Args:
            model_name: 模型名称
            
        Returns:
            Dict: 停止结果
        """
        try:
            model_config = get_model_config(model_name)
            if not model_config:
                return {
                    "success": False,
                    "message": f"模型配置不存在: {model_name}",
                    "model_name": model_name
                }
            
            container_name = model_config["container_name"]
            
            # 停止容器
            stop_cmd = ["docker", "stop", container_name]
            stop_result = subprocess.run(stop_cmd, capture_output=True, text=True)
            
            # 删除容器
            rm_cmd = ["docker", "rm", container_name]
            rm_result = subprocess.run(rm_cmd, capture_output=True, text=True)
            
            # 从活跃服务中移除
            if model_name in self.active_services:
                del self.active_services[model_name]
            
            logger.info(f"✅ 模型 {model_name} 服务已停止")
            return {
                "success": True,
                "message": f"模型 {model_name} 服务已停止",
                "model_name": model_name
            }
            
        except Exception as e:
            logger.error(f"停止模型 {model_name} 服务时出错: {str(e)}")
            return {
                "success": False,
                "message": f"停止模型 {model_name} 服务时出错: {str(e)}",
                "model_name": model_name
            }
    
    def is_service_running(self, port: int) -> bool:
        """检查指定端口的服务是否运行"""
        try:
            response = requests.get(f"http://localhost:{port}/health", timeout=5)
            return response.status_code == 200
        except:
            return False
    
    def _wait_for_service_ready(self, port: int, max_wait_time: int = 300) -> bool:
        """等待服务就绪"""
        logger.info(f"等待服务就绪，端口: {port}")
        check_interval = 10
        waited_time = 0
        
        while waited_time < max_wait_time:
            if self.is_service_running(port):
                logger.info(f"✅ 服务就绪，端口: {port}")
                return True
            else:
                logger.info(f"⏳ 服务尚未就绪，等待中... ({waited_time}/{max_wait_time}秒)")
                time.sleep(check_interval)
                waited_time += check_interval
        
        logger.error(f"❌ 服务启动超时，端口: {port}")
        return False
    
    def verify_model_loaded(self, model_name: str) -> Dict[str, Any]:
        """验证模型是否正确加载"""
        try:
            model_config = get_model_config(model_name)
            if not model_config:
                return {
                    "success": False,
                    "message": f"模型配置不存在: {model_name}"
                }
            
            port = model_config["port"]
            lora_module_name = model_config["lora_module_name"]
            
            response = requests.get(f"http://localhost:{port}/v1/models", timeout=10)
            
            if response.status_code == 200:
                models_data = response.json()
                available_models = [model.get('id', '') for model in models_data.get('data', [])]
                
                if lora_module_name in available_models:
                    return {
                        "success": True,
                        "message": f"模型 {model_name} 已正确加载",
                        "available_models": available_models
                    }
                else:
                    return {
                        "success": False,
                        "message": f"模型 {model_name} 未找到，可用模型: {available_models}",
                        "available_models": available_models
                    }
            else:
                return {
                    "success": False,
                    "message": f"无法获取模型列表，状态码: {response.status_code}"
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"验证模型时出错: {str(e)}"
            }
    
    def get_active_services(self) -> Dict[str, Any]:
        """获取所有活跃的服务"""
        return self.active_services.copy()
    
    def call_model_api(self, model_name: str, messages: List[Dict[str, str]], 
                      max_tokens: int = 2000, max_retries: int = 3) -> str:
        """
        调用指定模型的API
        
        Args:
            model_name: 模型名称
            messages: 消息列表
            max_tokens: 最大token数
            max_retries: 最大重试次数
            
        Returns:
            str: API响应内容
        """
        try:
            model_config = get_model_config(model_name)
            if not model_config:
                logger.error(f"模型配置不存在: {model_name}")
                return ""
            
            port = model_config["port"]
            lora_module_name = model_config["lora_module_name"]
            url = f"http://localhost:{port}/v1/chat/completions"
            
            # 获取可用模型列表
            try:
                response = requests.get(f"http://localhost:{port}/v1/models", timeout=10)
                if response.status_code == 200:
                    models_data = response.json()
                    available_models = [model.get('id', '') for model in models_data.get('data', [])]
                    
                    if lora_module_name in available_models:
                        model_name_for_api = lora_module_name
                    elif available_models:
                        model_name_for_api = available_models[0]
                        logger.warning(f"LoRA模型 {lora_module_name} 未找到，使用第一个可用模型: {model_name_for_api}")
                    else:
                        model_name_for_api = lora_module_name
                        logger.warning(f"未找到可用模型，使用默认模型: {model_name_for_api}")
                else:
                    model_name_for_api = lora_module_name
                    logger.warning(f"获取模型列表失败，使用默认模型: {model_name_for_api}")
            except Exception as e:
                model_name_for_api = lora_module_name
                logger.warning(f"获取模型列表出错: {str(e)}，使用默认模型: {model_name_for_api}")
            
            payload = {
                "model": model_name_for_api,
                "messages": messages,
                "max_tokens": max_tokens,
                "temperature": 0.0,
                "stream": False
            }
            
            # 重试机制
            for attempt in range(max_retries):
                try:
                    logger.info(f"尝试调用API (第{attempt + 1}次)...")
                    response = requests.post(url, json=payload, timeout=300)
                    
                    if response.status_code == 200:
                        result = response.json()
                        return result["choices"][0]["message"]["content"]
                    else:
                        logger.error(f"API调用失败，状态码: {response.status_code}")
                        if attempt < max_retries - 1:
                            logger.info(f"等待5秒后重试...")
                            time.sleep(5)
                            continue
                        else:
                            return ""
                            
                except requests.exceptions.ConnectionError as e:
                    logger.error(f"连接错误 (第{attempt + 1}次): {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"等待10秒后重试...")
                        time.sleep(10)
                        continue
                    else:
                        return ""
                        
                except requests.exceptions.Timeout as e:
                    logger.error(f"请求超时 (第{attempt + 1}次): {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"等待15秒后重试...")
                        time.sleep(15)
                        continue
                    else:
                        return ""
                        
                except Exception as e:
                    logger.error(f"API调用出错 (第{attempt + 1}次): {str(e)}")
                    if attempt < max_retries - 1:
                        logger.info(f"等待5秒后重试...")
                        time.sleep(5)
                        continue
                    else:
                        return ""
            
            return ""
            
        except Exception as e:
            logger.error(f"调用模型API时出错: {str(e)}")
            return ""

# 全局服务管理器实例
vllm_service_manager = VLLMServiceManager()

# 便捷函数
def start_model_service(model_name: str, force_restart: bool = False) -> Dict[str, Any]:
    """启动模型服务的便捷函数"""
    return vllm_service_manager.start_model_service(model_name, force_restart)

def stop_model_service(model_name: str) -> Dict[str, Any]:
    """停止模型服务的便捷函数"""
    return vllm_service_manager.stop_model_service(model_name)

def call_model_api(model_name: str, messages: List[Dict[str, str]], 
                  max_tokens: int = 2000, max_retries: int = 3) -> str:
    """调用模型API的便捷函数"""
    return vllm_service_manager.call_model_api(model_name, messages, max_tokens, max_retries) 
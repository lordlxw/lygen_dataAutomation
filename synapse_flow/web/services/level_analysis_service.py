#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
层级分析服务
用于处理JSON数组的层级判断任务
"""

import json
import time
import requests
import re
import datetime
import os
import subprocess
from typing import List, Dict, Any
from synapse_flow.db import get_pg_conn
from vllm_service_manager import start_model_service, call_model_api
from model_config import get_model_config

class LevelAnalysisService:
    """层级分析服务"""
    
    def __init__(self, port: int = 8202):
        # 获取level_model的配置
        self.model_config = get_model_config("level_model")
        if self.model_config:
            self.port = self.model_config["port"]
            self.base_url = f"http://localhost:{self.port}"
        else:
            # 如果配置不存在，使用默认端口
            self.port = port
            self.base_url = f"http://localhost:{self.port}"
        
        self.confirmed_levels = []  # 存储已确认的层级信息
        self.level_path_stack = []  # 存储当前活跃的层级路径栈
        
        # 新增：层级上下文传递相关
        self.level_sequence = []  # 存储层级序列 [1, 2, 3, 3, 4, 4, 5, 2, 3, 3]
        self.context_paths = []   # 存储每个位置的上文路径 [[], [0], [0,1], [0,2], ...]
        self.current_context = [] # 当前维护的上下文路径
        
        # 创建日志目录
        self.log_dir = "level_analysis_logs"
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)
        
        # 生成日志文件名
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        self.log_file = os.path.join(self.log_dir, f"level_analysis_log_{timestamp}.json")
        self.detailed_log = []  # 存储详细日志
    
    def log_step(self, step_name: str, data: Dict[str, Any]):
        """记录处理步骤到日志"""
        # 只记录处理完成的结果，格式参考参考JSON
        if step_name == "处理完成":
            log_entry = {
                "original_data": {
                    "text": data["final_result"]["text"],
                    "isTitleMarked": data["final_result"]["isTitleMarked"]
                },
                "ai_response": data["final_result"].get("ai_response", ""),
                "parsed_result": {
                    "level": data["final_result"]["level"],
                    "reasoning": data["final_result"]["reasoning"],
                    "is_special_case": data["final_result"]["is_special_case"],
                    "special_type": data["final_result"]["special_type"]
                },
                "context_path": data["final_result"]["context_path"],
                "model_info": {
                    "model_name": "llama3.1_8b",
                    "base_model": "Meta-Llama-3.1-8B-Instruct",
                    "lora_path": "/home/liuxinwei/Models/层级训练",
                    "api_endpoint": "http://localhost:8202/v1/chat/completions"
                },
                "prompt_details": data.get("prompt_details", {}),  # 新增：prompt详情
                "timestamp": datetime.datetime.now().isoformat()
            }
            self.detailed_log.append(log_entry)
            
            # 实时写入文件
            try:
                with open(self.log_file, 'w', encoding='utf-8') as f:
                    json.dump(self.detailed_log, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"写入日志文件失败: {str(e)}")
    
    def get_log_file_path(self) -> str:
        """获取日志文件路径"""
        return self.log_file
    
    def check_vllm_service_status(self) -> bool:
        """检查vLLM服务状态"""
        try:
            # 使用正确的端口检查服务状态
            response = requests.get(f"{self.base_url}/v1/models", timeout=10)
            if response.status_code == 200:
                print(f"✓ vLLM服务正常运行 (端口: {self.port})")
                return True
            else:
                print(f"✗ vLLM服务异常，状态码: {response.status_code} (端口: {self.port})")
                return False
        except Exception as e:
            print(f"✗ vLLM服务连接失败 (端口: {self.port}): {str(e)}")
            return False
    
    def call_vllm_api(self, messages: List[Dict[str, str]], max_tokens: int = 2000, max_retries: int = 3) -> str:
        """调用vLLM API，专用于层级分析"""
        # 启动服务（如果没启动）
        start_level_vllm_service()
        
        # 打印调用的模型信息
        print(f"\n=== vLLM API调用信息 ===")
        print(f"调用的微调模型: llama3.1_8b (LoRA微调模型)")
        print(f"基础模型: Meta-Llama-3.1-8B-Instruct")
        print(f"LoRA权重路径: /home/liuxinwei/Models/层级训练")
        print(f"API端点: http://localhost:8202/v1/chat/completions")
        
        # 调用API
        url = f"http://localhost:8202/v1/chat/completions"
        payload = {
            "model": "llama3.1_8b",  # 修改为与vLLM服务启动时一致的模型名称
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.0,
            "stream": False
        }
        
        # 打印传入的prompt详情
        print(f"\n=== 传入的Prompt详情 ===")
        for i, msg in enumerate(messages):
            print(f"消息{i+1} ({msg['role']}):")
            print(f"内容: {msg['content']}")
            print("-" * 50)
        
        for attempt in range(max_retries):
            try:
                print(f"\n=== API调用 (第{attempt + 1}次) ===")
                print(f"请求URL: {url}")
                print(f"请求参数: model={payload['model']}, max_tokens={payload['max_tokens']}, temperature={payload['temperature']}")
                
                response = requests.post(url, json=payload, timeout=300)
                print(f"响应状态码: {response.status_code}")
                
                if response.status_code == 200:
                    result = response.json()
                    ai_response = result["choices"][0]["message"]["content"]
                    
                    print(f"\n=== API返回结果 ===")
                    print(f"AI响应内容: {ai_response}")
                    print(f"响应token使用: {result.get('usage', {}).get('total_tokens', '未知')}")
                    print("=" * 80)
                    
                    return ai_response
                else:
                    print(f"API调用失败，状态码: {response.status_code}")
                    print(f"错误响应: {response.text}")
                    if attempt < max_retries - 1:
                        print(f"等待5秒后重试...")
                        time.sleep(5)
                        continue
                    else:
                        print("所有重试次数已用完，返回空字符串")
                        return ""
            except Exception as e:
                print(f"API调用出错 (第{attempt + 1}次): {str(e)}")
                if attempt < max_retries - 1:
                    print(f"等待5秒后重试...")
                    time.sleep(5)
                    continue
                else:
                    print("所有重试次数已用完，返回空字符串")
                    return ""
        return ""
    
    def update_level_path_stack(self, new_level: int, item_data: Dict[str, Any]):
        """
        更新层级路径栈
        实现"递归向上路径"算法
        """
        # 创建新的层级信息
        level_info = {
            "text": item_data["text"],
            "isTitleMarked": item_data["isTitleMarked"],
            "level": new_level,
            "index": len(self.confirmed_levels),  # 记录在confirmed_levels中的索引
            "special_type": item_data.get("special_type")  # 添加special_type信息
        }
        
        # 更新层级路径栈
        # 1. 移除所有大于等于新层级的节点
        self.level_path_stack = [node for node in self.level_path_stack if node["level"] < new_level]
        
        # 2. 添加新节点
        self.level_path_stack.append(level_info)
        
        print(f"更新层级路径栈: {[node['level'] for node in self.level_path_stack]}")
        print(f"更新层级路径栈索引: {[node['index'] for node in self.level_path_stack]}")
        print(f"更新层级路径栈special_type: {[node.get('special_type', 'None') for node in self.level_path_stack]}")
    
    def get_context_path(self) -> List[Dict[str, Any]]:
        """
        获取当前活跃的层级路径（递归父级路径）
        返回递归向上路径中的节点
        注意：这里返回的是处理当前文本之前的路径，不包含当前文本
        """
        context_path = []
        for node in self.level_path_stack:
            # 如果是同属以往层级，补充同级的上一个非特殊节点
            if node.get("special_type") == "同属以往层级":
                for i in range(node["index"]-1, -1, -1):
                    if (self.confirmed_levels[i]["level"] == node["level"] and
                        self.confirmed_levels[i].get("special_type") != "同属以往层级"):
                        # 只补充一次
                        context_path.append({
                            "text": self.confirmed_levels[i]["text"],
                            "isTitleMarked": self.confirmed_levels[i]["isTitleMarked"],
                            "level": self.confirmed_levels[i]["level"],
                            "index": i
                        })
                        break
            # 正常递归路径
            context_path.append(node)
        return context_path
    
    def get_extended_context_path(self) -> List[Dict[str, Any]]:
        """
        获取扩展的上下文路径
        当上一个结果是"同属以往层级"时，需要带上更早的层级信息
        """
        extended_path = []
        
        print(f"DEBUG: 开始扩展上下文路径，confirmed_levels长度: {len(self.confirmed_levels)}")
        print(f"DEBUG: confirmed_levels: {[(i, level.get('special_type', 'None')) for i, level in enumerate(self.confirmed_levels)]}")
        
        # 当上一个结果是"同属以往层级"时，应该带上：
        # 1. 之前所有层级（按时间顺序）
        # 2. 当前这个"同属以往层级"的层级
        
        # 遍历所有confirmed_levels（除了最后一个，因为最后一个就是"同属以往层级"）
        for i in range(len(self.confirmed_levels) - 1):
            level_info = self.confirmed_levels[i]
            print(f"DEBUG: 添加索引{i}: special_type={level_info.get('special_type', 'None')}")
            extended_path.append({
                "text": level_info["text"],
                "isTitleMarked": level_info["isTitleMarked"],
                "level": level_info["level"],
                "index": i
            })
        
        # 再加上当前这个"同属以往层级"的层级
        if self.confirmed_levels:
            last_level = self.confirmed_levels[-1]
            print(f"DEBUG: 添加最后一个层级，索引{len(self.confirmed_levels) - 1}")
            extended_path.append({
                "text": last_level["text"],
                "isTitleMarked": last_level["isTitleMarked"],
                "level": last_level["level"],
                "index": len(self.confirmed_levels) - 1
            })
        
        print(f"DEBUG: 扩展上下文路径: {[node['level'] for node in extended_path]}")
        print(f"DEBUG: 扩展上下文路径索引: {[node['index'] for node in extended_path]}")
        return extended_path
    
    def build_level_prompt(self, target_item: Dict[str, Any]) -> tuple:
        """构建层级判断的prompt"""
        
        def truncate_text(text: str) -> str:
            """截断文本到第一个逗号或句号为止"""
            # 查找第一个逗号或句号的位置
            comma_pos = text.find('，')
            period_pos = text.find('。')
            
            if comma_pos == -1 and period_pos == -1:
                return text  # 没有找到逗号或句号，返回原文本
            
            if comma_pos == -1:
                return text[:period_pos]  # 只有句号
            elif period_pos == -1:
                return text[:comma_pos]   # 只有逗号
            else:
                # 取较小的位置（更早出现的标点）
                return text[:min(comma_pos, period_pos)]
        
        system_prompt = """你是一个文本层级梳理专家，你的任务是**判断目标文本块的层级**

你会得到四种信息，辅助你确定目标文本块的层级：
1. 目标文本块的内容：这个文本块的内容是一个层级的开头部分，具有层级的内容与结构特征，可以方便你进行判断；
2. 层级主题：
（1）结构层级：这里指的从这里开始可能是一个结构层级，是整个文章的书写框架结构标题；
（2）段落层级：这里指的从这里开始可能是一个段落层级，是结构下文章段落内的叙述标题；
3. 目标文本块的前文相关递进层级：通过对比以往的层级，辅助你判断目标文本块作为层级开头应该属于什么层级；

**注意**：文本的层级计数永远都是按序往后计数的，绝对不可能出现后文的层级数字比前文小，如果出现说明不是同一层级！

回答格式：
如果是正常情况判断请回答：因为目标文本开头是前文XXX，且XXX，所以判断为{层级X}。
如果是特殊情况判断请回答：因为目标文本开头是前文XXX，且XXX，但我认为{特殊原因}，所以判断为{层级X}。
特殊原因：
（1）层级主题错误：段落和结构层级混淆；
（2）同属以往层级：是新层级格式，但是明显不属于上一级层级；
（3）层级顺序混乱：是旧层级格式，但不是往下计数，说明不是同一层级；
（4）总分结构后的总层级：总分总后的总结语句，一般属于当前同一层级；
（5）层级为模版内部层级：层级标记为99级，特指文本内部的模版内部层级。

举例1：
我们已经确认：
结构层级1："企业设立管理"，

请问结构层级： "第一节 登记信息确认",是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且与上一级同为结构层级，所以判断为{层级2}。

举例2：
我们已经确认：
结构层级1："中华人民共和国个人所得税法"，
结构层级2："第一条【征税范围】"，

请问段落层级： "第一条在中国境内有住所，...",是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是上一级结构层级下的第一个段落层级，所以判断为{层级3}。

举例3：
我们已经确认：
段落层级1："28.资源综合利用企业受到税务处罚能否继续享受增值税即征即退？"，
段落层级2："问题："，
段落层级3： "（2）依据《税收征收管理法》第六十八条规定，..."，
段落层级3："回复："，

请问段落层级： "（1）集团公司采取资金池模式集中资金管理是常规做法，...",是第几层级的开头内容？

回答：因为目标文本开头是前文具有的一种旧层级格式，且与上一级同为段落层级，但我认为{层级顺序混乱}，所以判断为{层级4}。

举例4：
我们已经确认：
结构层级1："第一章 税收的产生与发展"，
段落层级2："内容提要：税收是政府为了实现其职能、满足社会公共需要，..."，
段落层级2："导入案例：西方税与东方税的交汇 国王与税收 "，

请问结构层级："第一节 税收词源",是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是段落层级回到结构层级，但我认为{同属以往层级}，所以判断为{层级2}。

举例5：
我们已经确认：
结构层级1："第一章 税收的产生与发展"，
结构层级2："第二节 税收的产生"，
结构层级3："一、税收产生的前提条件"，
段落层级4： "3.社会条件：经常化的社会公共需要出现"，
段落层级5："（4）社会公共需要是一种具有外部性的需求，..."，

请问段落层级： "这些特征确定了社会公共需求是一个社会需求不可或缺的部分，...",是第几层级的开头内容？

回答：因为目标文本开头是前文具有的一种旧层级格式，且与上一级同为段落层级，但我认为{总分结构后的总层级}，所以判断为{层级5}。

举例6：
我们已经确认：
结构层级1："税款缴纳及退税管理"，
结构层级2："第二节 退（抵）税"，
结构层级3："四、留抵退税"，
结构层级4："（一）一般企业留抵退税"，
段落层级5： "3.操作办法。"，
段落层级6："（5）税务机关对增值税涉税风险疑点进行排查时，..."，

请问结构层级： "4.附加税费的处理",是第几层级的开头内容？

回答：因为目标文本开头是前文具有的一种旧层级格式，且是段落层级回到结构层级，但我认为{层级主题错误}，所以判断为{层级5}。"""
        
        system_prompt = system_prompt.strip() + "\n请注意，第一条文本一定是层级1，后续层级在此基础上顺延。"
        
        # 构建递归向上路径的上下文信息
        context_info = ""
        context_path = self.get_context_path()
        
        if context_path:
            context_info = "我们已经确认：\n"
            for i, level_info in enumerate(context_path):
                level_type = "结构层级" if level_info["isTitleMarked"] == "section level" else "段落层级"
                truncated_text = truncate_text(level_info["text"])
                context_info += f"{level_type}{level_info['level']}：\"{truncated_text}\"，\n"
            context_info += "\n"
        
        # 构建目标文本块信息
        target_type = "结构层级" if target_item["isTitleMarked"] == "section level" else "段落层级"
        truncated_target_text = truncate_text(target_item["text"])
        target_info = f"请问{target_type}： \"{truncated_target_text}\",是第几层级的开头内容？"
        
        # 按照新格式构建user_prompt
        user_prompt = f"需要分析的这段语句是:\"{context_info}{target_info}\"\n请根据问题要求与问题进行回答"
        
        return system_prompt, user_prompt
    
    def parse_level_response(self, response: str) -> Dict[str, Any]:
        """解析AI返回的层级判断结果"""
        result = {
            "level": None,
            "reasoning": response,
            "is_special_case": False,
            "special_type": None
        }
        
        try:
            # 提取层级数字 - 支持多种格式
            # 1. 尝试匹配 {层级X} 格式
            level_match = re.search(r'\{层级([一二三四五六七八九十\d]+)\}', response)
            if level_match:
                level_str = level_match.group(1)
                # 转换中文数字为阿拉伯数字
                chinese_to_arabic = {
                    '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
                    '六': 6, '七': 7, '八': 8, '九': 9, '十': 10
                }
                if level_str in chinese_to_arabic:
                    result["level"] = chinese_to_arabic[level_str]
                else:
                    result["level"] = int(level_str)
            
            # 2. 如果上面没匹配到，尝试匹配 层级X 格式
            if result["level"] is None:
                level_match = re.search(r'层级(\d+)', response)
                if level_match:
                    result["level"] = int(level_match.group(1))
            
            # 3. 如果还没匹配到，尝试匹配 第X层级 格式
            if result["level"] is None:
                level_match = re.search(r'第([一二三四五六七八九十\d]+)层级', response)
                if level_match:
                    level_str = level_match.group(1)
                    chinese_to_arabic = {
                        '一': 1, '二': 2, '三': 3, '四': 4, '五': 5,
                        '六': 6, '七': 7, '八': 8, '九': 9, '十': 10
                    }
                    if level_str in chinese_to_arabic:
                        result["level"] = chinese_to_arabic[level_str]
                    else:
                        result["level"] = int(level_str)
            
            # 判断是否为特殊情况并提取具体类型
            if "但我认为" in response:
                result["is_special_case"] = True
                # 提取特殊情况类型
                special_match = re.search(r'但我认为\{([^}]+)\}', response)
                if special_match:
                    result["special_type"] = special_match.group(1)
                else:
                    # 兼容旧格式和新格式
                    if "层级主题错误" in response:
                        result["special_type"] = "层级主题错误"
                    elif "层级顺序混乱" in response or "顺序混乱" in response:
                        result["special_type"] = "层级顺序混乱"
                    elif "同属以往层级" in response:
                        result["special_type"] = "同属以往层级"
                    elif "总分结构后的总层级" in response:
                        result["special_type"] = "总分结构后的总层级"
                    elif "层级为模版内部层级" in response:
                        result["special_type"] = "层级为模版内部层级"
                    elif "层级格式不同却语义相关" in response:
                        result["special_type"] = "层级格式不同却语义相关"
            
        except Exception as e:
            print(f"解析层级响应时出错: {str(e)}")
        
        return result
    
    def process_single_item(self, item_data: Dict[str, Any]) -> Dict[str, Any]:
        """处理单个数据项的层级判断"""
        try:
            # 获取调用AI之前的context_path
            context_path_before = self.get_context_path()
            
            # 检查是否是第一条数据
            is_first_item = len(self.confirmed_levels) == 0
            
            if is_first_item:
                # 第一条数据直接设置为层级1，不调用AI
                print(f"\n{'='*80}")
                print(f"=== 处理第一条数据项 ===")
                print(f"数据ID: {item_data.get('id', '未知')}")
                print(f"目标文本: {item_data['text']}")
                print(f"标记类型: {item_data['isTitleMarked']}")
                print(f"自动设置为层级1（第一条数据）")
                print(f"{'='*80}")
                
                # 直接设置层级1
                parsed_result = {
                    "level": 1,
                    "reasoning": "第一条文本自动设置为层级1",
                    "is_special_case": False,
                    "special_type": None
                }
                
                # 更新层级路径栈、层级序列和层级上下文传递
                current_index = len(self.level_sequence)  # 当前元素在层级序列中的索引
                current_context_path = self.current_context.copy()
                
                # 更新层级路径栈
                item_data_with_special = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "special_type": parsed_result["special_type"]
                }
                self.update_level_path_stack(parsed_result['level'], item_data_with_special)
                
                # 更新层级序列
                self.level_sequence.append(parsed_result['level'])
                
                # 更新层级上下文传递
                self.update_hierarchical_context(parsed_result['level'], current_index)
                
                # 添加到已确认层级列表
                confirmed_level_info = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result["level"],
                    "reasoning": parsed_result["reasoning"],
                    "is_special_case": parsed_result["is_special_case"],
                    "special_type": parsed_result["special_type"],
                    "context_path": current_context_path
                }
                self.confirmed_levels.append(confirmed_level_info)
                
                # 记录处理完成
                self.log_step("处理完成", {
                    "final_result": {
                        "id": item_data.get("id"),
                        "text": item_data["text"],
                        "isTitleMarked": item_data["isTitleMarked"],
                        "level": parsed_result["level"],
                        "reasoning": parsed_result["reasoning"],
                        "is_special_case": parsed_result["is_special_case"],
                        "special_type": parsed_result["special_type"],
                        "context_path": current_context_path,
                        "ai_response": "第一条数据，未调用AI"
                    },
                    "prompt_details": {
                        "system_prompt": "第一条数据，未调用AI",
                        "user_prompt": "第一条数据，未调用AI",
                        "messages": []
                    }
                })
                
                # 返回处理结果
                return {
                    "id": item_data.get("id"),
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result["level"],
                    "reasoning": parsed_result["reasoning"],
                    "is_special_case": parsed_result["is_special_case"],
                    "special_type": parsed_result["special_type"],
                    "ai_response": "第一条数据，未调用AI",
                    "context_path": current_context_path
                }
            
            # 从第二条开始，正常调用AI进行层级判断
            # 构建prompt
            system_prompt, user_prompt = self.build_level_prompt(item_data)
            
            # 构建消息
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            
            print(f"\n{'='*80}")
            print(f"=== 处理数据项 ===")
            print(f"数据ID: {item_data.get('id', '未知')}")
            print(f"目标文本: {item_data['text']}")
            print(f"标记类型: {item_data['isTitleMarked']}")
            print(f"当前层级路径栈: {[node['level'] for node in self.level_path_stack]}")
            print(f"当前层级序列: {self.level_sequence}")
            print(f"当前层级上下文: {self.current_context}")
            print(f"{'='*80}")
            
            # 检查vLLM服务状态
            if not self.check_vllm_service_status():
                print("vLLM服务不可用，跳过当前处理")
                return {
                    "id": item_data.get("id"),
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": None,
                    "reasoning": "vLLM服务不可用",
                    "is_special_case": False,
                    "special_type": None,
                    "ai_response": "",
                    "context_path": []
                }
            
            # 调用API
            ai_response = self.call_vllm_api(messages)
            
            # 解析响应
            parsed_result = self.parse_level_response(ai_response)
            
            print(f"\n=== 解析结果 ===")
            print(f"解析结果: 层级{parsed_result['level']}")
            if parsed_result['is_special_case']:
                print(f"特殊情况: {parsed_result['special_type']}")
            print(f"推理过程: {parsed_result['reasoning']}")
            print(f"{'='*80}")
            
            # 如果成功解析到层级，更新层级路径栈、层级序列和层级上下文传递
            if parsed_result['level'] is not None:
                current_index = len(self.level_sequence)  # 当前元素在层级序列中的索引
                
                # 获取实际传给AI的上下文路径（在更新之前）
                actual_context_path = self.get_context_path()
                # 将实际上下文路径转换为索引列表
                actual_context_indices = [node['index'] for node in actual_context_path]
                
                # 更新层级路径栈（原有逻辑）
                item_data_with_special = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "special_type": parsed_result["special_type"]
                }
                self.update_level_path_stack(parsed_result['level'], item_data_with_special)
                
                # 新增：更新层级序列
                self.level_sequence.append(parsed_result['level'])
                
                # 新增：更新层级上下文传递
                self.update_hierarchical_context(parsed_result['level'], current_index)
                
                # 添加到已确认层级列表
                confirmed_level_info = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result["level"],
                    "reasoning": parsed_result["reasoning"],
                    "is_special_case": parsed_result["is_special_case"],
                    "special_type": parsed_result["special_type"],
                    "context_path": actual_context_indices  # 使用实际传给AI的上下文路径
                }
                self.confirmed_levels.append(confirmed_level_info)
            
            # 记录处理完成（使用简化格式）
            self.log_step("处理完成", {
                "final_result": {
                    "id": item_data.get("id"),
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result["level"],
                    "reasoning": parsed_result["reasoning"],
                    "is_special_case": parsed_result["is_special_case"],
                    "special_type": parsed_result["special_type"],
                    "context_path": actual_context_indices if parsed_result['level'] is not None else [],
                    "ai_response": ai_response
                },
                "prompt_details": {
                    "system_prompt": system_prompt,
                    "user_prompt": user_prompt,
                    "messages": messages
                }
            })
            
            # 返回处理结果
            return {
                "id": item_data.get("id"),
                "text": item_data["text"],
                "isTitleMarked": item_data["isTitleMarked"],
                "level": parsed_result["level"],
                "reasoning": parsed_result["reasoning"],
                "is_special_case": parsed_result["is_special_case"],
                "special_type": parsed_result["special_type"],
                "ai_response": ai_response,
                "context_path": actual_context_indices if parsed_result['level'] is not None else []  # 使用实际传给AI的上下文路径
            }
            
        except Exception as e:
            error_msg = f"处理单个数据项时出错: {str(e)}"
            print(error_msg)
            
            return {
                "id": item_data.get("id"),
                "text": item_data["text"],
                "isTitleMarked": item_data["isTitleMarked"],
                "level": None,
                "reasoning": f"处理出错: {str(e)}",
                "is_special_case": False,
                "special_type": None,
                "ai_response": "",
                "context_path": []
            }
    
    def process_batch(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量处理数据列表"""
        results = []
        
        for i, item_data in enumerate(data_list):
            print(f"处理第 {i+1}/{len(data_list)} 条数据...")
            
            result = self.process_single_item(item_data)
            results.append(result)
        
        # 打印层级分析摘要
        self.print_hierarchy_analysis_summary()
        
        return results
    
    def get_confirmed_levels(self) -> List[Dict[str, Any]]:
        """获取已确认的层级列表"""
        return self.confirmed_levels.copy()
    
    def get_level_sequence(self) -> List[int]:
        """获取层级序列"""
        return [level_info["level"] for level_info in self.confirmed_levels]
    
    def get_hierarchical_contexts(self) -> List[List[int]]:
        """
        获取所有位置的层级上下文
        返回一个列表，其中每个子列表是对应元素（按其在原序列中的下标）的层级上下文（上文）
        """
        return [path.copy() for path in self.context_paths]
    
    def get_level_sequence_with_contexts(self) -> Dict[str, Any]:
        """
        获取层级序列和对应的上下文信息
        返回包含层级序列和每个位置上下文的完整信息
        """
        return {
            "level_sequence": self.level_sequence.copy(),
            "context_paths": self.context_paths.copy(),
            "current_context": self.current_context.copy(),
            "confirmed_levels": self.confirmed_levels.copy()
        }
    
    def print_hierarchy_analysis_summary(self):
        """
        打印层级分析摘要
        显示层级序列和每个位置的上下文信息
        """
        print("\n" + "="*60)
        print("层级分析摘要")
        print("="*60)
        print(f"层级序列: {self.level_sequence}")
        print(f"当前上下文: {self.current_context}")
        print("\n各位置上下文:")
        for i, context_path in enumerate(self.context_paths):
            level = self.level_sequence[i] if i < len(self.level_sequence) else "未知"
            text = self.confirmed_levels[i]["text"][:30] + "..." if i < len(self.confirmed_levels) else "未知"
            print(f"  位置{i}: 层级{level} - 上文{context_path} - 文本: {text}")
        print("="*60)

    def update_hierarchical_context(self, new_level: int, current_index: int):
        """
        更新层级上下文传递
        实现层级结构传递算法
        """
        # 记录更新前的状态
        self.log_step("层级上下文更新前", {
            "current_context": self.current_context.copy(),
            "new_level": new_level,
            "current_index": current_index,
            "level_sequence": self.level_sequence.copy()
        })
        
        # 1. 记录当前元素的"上文"（处理当前元素之前的context）
        self.context_paths.append(self.current_context.copy())
        
        # 2. 根据层级关系更新 current_context
        if not self.current_context:  # a. 如果 current_context 为空 (处理第一个元素)
            self.current_context.append(current_index)
        else:
            # 获取上一个在 current_context 中的元素的层级值
            previous_level_index_in_context = self.current_context[-1]
            previous_level_value = self.level_sequence[previous_level_index_in_context]
            
            if new_level == previous_level_value:  # b. 层级相等 (同级元素)
                # 替换最后一个元素
                self.current_context[-1] = current_index
            elif new_level > previous_level_value:  # c. 层级递增 (深入子级)
                # 追加到末尾
                self.current_context.append(current_index)
            else:  # new_level < previous_level_value // d. 层级骤降 (跳出父级)
                matched_parent_index_in_context = -1
                
                # 从 current_context 末尾向前查找匹配的父级
                for i in range(len(self.current_context) - 1, -1, -1):
                    ancestor_index = self.current_context[i]
                    ancestor_level_value = self.level_sequence[ancestor_index]
                    
                    if ancestor_level_value <= new_level:
                        matched_parent_index_in_context = i
                        break  # 找到第一个匹配的就停止
                
                if matched_parent_index_in_context != -1:
                    # 截断 current_context 到 matched_parent_index_in_context 的前面（不包含它）
                    # 然后将 current_index 追加到新的 context 列表的末尾
                    self.current_context = self.current_context[:matched_parent_index_in_context]
                    self.current_context.append(current_index)
                else:
                    # 如果没有找到匹配的父级，清空并只保留当前
                    self.current_context.clear()
                    self.current_context.append(current_index)
        
        # 记录更新后的状态
        self.log_step("层级上下文更新后", {
            "updated_context": self.current_context.copy(),
            "context_paths": self.context_paths.copy(),
            "level_sequence": self.level_sequence.copy()
        })
        
        print(f"层级上下文更新: 新层级={new_level}, 当前索引={current_index}")
        print(f"  更新前context: {self.context_paths[-1] if self.context_paths else []}")
        print(f"  更新后context: {self.current_context}")
    
    def get_current_context_path(self) -> List[int]:
        """
        获取当前的上文路径
        返回当前元素的上文（不包含当前元素）
        """
        return self.current_context.copy()
    
    def get_context_path_for_index(self, index: int) -> List[int]:
        """
        获取指定索引位置的上文路径
        """
        if 0 <= index < len(self.context_paths):
            return self.context_paths[index].copy()
        return []

    def print_tree_view(self):
        """
        打印树状缩进的层级结构（带特殊标记）
        """
        stack = []
        for i, node in enumerate(self.confirmed_levels):
            level = node["level"]
            text = node["text"][:40] + ("..." if len(node["text"]) > 40 else "")
            special = node.get("special_type")
            marker = "⚠" if special == "同属以往层级" else "✓"
            special_info = f" ({special})" if special else ""
            # 维护栈
            while stack and self.confirmed_levels[stack[-1]]["level"] >= level:
                stack.pop()
            indent = "  " * len(stack)
            print(f"{indent}{marker} 层级{level}: {text}{special_info}")
            stack.append(i)

def update_pdf_json_hierarchy(data_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    更新pdf_json表中的层级信息
    
    Args:
        data_list: 包含id、text、isTitleMarked等字段的数据列表
        
    Returns:
        Dict: 更新结果
    """
    try:
        # 初始化层级分析服务
        level_service = LevelAnalysisService()
        
        # 处理数据
        results = level_service.process_batch(data_list)
        
        # 新增：调试用，输出树状结构
        level_service.print_tree_view()
        
        # 更新数据库
        conn = get_pg_conn()
        updated_count = 0
        
        try:
            with conn.cursor() as cur:
                for result in results:
                    if result["id"] and result["level"] is not None:
                        # 更新prompt_hierarchy和prompt_hierarchy_reason字段
                        cur.execute("""
                            UPDATE pdf_json 
                            SET prompt_hierarchy = %s, prompt_hierarchy_reason = %s
                            WHERE id = %s
                        """, (result["level"], result["reasoning"], result["id"]))
                        updated_count += 1
                        print(f"更新记录 ID {result['id']}: 层级 {result['level']}")
            
            conn.commit()
            
            return {
                "status": "success",
                "message": f"成功更新 {updated_count} 条记录",
                "total_processed": len(results),
                "updated_count": updated_count,
                "results": results,
                "log_file_path": level_service.get_log_file_path(),  # 返回日志文件路径
                "hierarchy_analysis": level_service.get_level_sequence_with_contexts()  # 新增：返回层级分析结果
            }
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
            
    except Exception as e:
        print(f"更新数据库时出错: {str(e)}")
        return {
            "status": "error",
            "message": f"更新失败: {str(e)}",
            "total_processed": len(data_list),
            "updated_count": 0,
            "results": []
        }

def analyze_hierarchy_by_run_id(run_id: str) -> Dict[str, Any]:
    """
    根据run_id从数据库查询数据并进行层级分析
    
    Args:
        run_id: 运行ID
        
    Returns:
        Dict: 分析结果
    """
    try:
        # 从数据库查询数据
        conn = get_pg_conn()
        data_list = []
        
        try:
            with conn.cursor() as cur:
                # 查询version=1且user_modified_level为1或2的数据，按id排序
                cur.execute("""
                    SELECT id, user_modified_text, user_modified_level
                    FROM pdf_json
                    WHERE run_id = %s AND version = 1 
                    AND user_modified_level IN (1, 2)
                    ORDER BY id ASC
                """, (run_id,))
                
                rows = cur.fetchall()
                
                if not rows:
                    return {
                        "status": "error",
                        "message": f"未找到run_id {run_id} 的version=1数据，或没有user_modified_level为1或2的记录",
                        "total_processed": 0,
                        "updated_count": 0,
                        "results": []
                    }
                
                print(f"找到 {len(rows)} 条需要分析的数据")
                
                # 转换为API格式
                for row in rows:
                    record_id, user_modified_text, user_modified_level = row
                    
                    # 根据user_modified_level确定isTitleMarked
                    if user_modified_level == 1:
                        is_title_marked = "section level"
                    elif user_modified_level == 2:
                        is_title_marked = "context level"
                    else:
                        # 跳过其他值
                        continue
                    
                    data_list.append({
                        "id": record_id,
                        "text": user_modified_text,
                        "isTitleMarked": is_title_marked
                    })
                
                print(f"准备分析 {len(data_list)} 条数据")
                
        finally:
            conn.close()
        
        if not data_list:
            return {
                "status": "error",
                "message": f"run_id {run_id} 没有有效的数据需要分析",
                "total_processed": 0,
                "updated_count": 0,
                "results": []
            }
        
        # 调用层级分析服务
        result = update_pdf_json_hierarchy(data_list)
        
        # 添加run_id信息到结果中
        result["run_id"] = run_id
        result["data_count"] = len(data_list)
        
        return result
        
    except Exception as e:
        print(f"根据run_id分析层级时出错: {str(e)}")
        return {
            "status": "error",
            "message": f"分析失败: {str(e)}",
            "run_id": run_id,
            "total_processed": 0,
            "updated_count": 0,
            "results": []
        }

def start_level_vllm_service():
    """
    启动专用于层级分析的vLLM服务（如已启动则跳过）
    """
    import requests
    import subprocess
    import time

    # 1. 检查端口服务是否已启动
    try:
        resp = requests.get("http://localhost:8202/v1/models", timeout=5)
        if resp.status_code == 200:
            print("层级分析vLLM服务已在运行，端口8202")
            return True
    except Exception:
        pass

    # 2. 检查容器是否存在（包括已停止的）
    result = subprocess.run(
        ["docker", "ps", "-a", "-q", "-f", "name=vllm_level"],
        stdout=subprocess.PIPE, text=True
    )
    
    if result.stdout.strip():
        # 容器存在，检查是否在运行
        running_result = subprocess.run(
            ["docker", "ps", "-q", "-f", "name=vllm_level"],
            stdout=subprocess.PIPE, text=True
        )
        if running_result.stdout.strip():
            print("vllm_level 容器已在运行，但服务未响应，建议手动检查容器日志。")
            return False
        else:
            # 容器存在但未运行，尝试启动
            print("尝试启动 vllm_level 容器...")
            subprocess.run(["docker", "start", "vllm_level"])
    else:
        # 容器不存在，需要创建
        print("vllm_level 容器不存在，自动创建...")
        
        # 获取模型配置
        model_config = get_model_config("level_model")
        if not model_config:
            print("❌ 无法获取 level_model 配置")
            return False
        
        # 构建 docker run 命令
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
        
        print(f"执行命令: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ 创建容器失败: {result.stderr}")
            return False
        
        print("✅ vllm_level 容器创建成功")

    # 4. 等待服务端口可用（延长等待时间到5分钟）
    print("等待服务启动（最多5分钟）...")
    for i in range(30):  # 30次，每次10秒，总共5分钟
        try:
            resp = requests.get("http://localhost:8202/v1/models", timeout=3)
            if resp.status_code == 200:
                print("✅ vLLM服务已成功启动")
                return True
        except Exception:
            pass
        time.sleep(10)
        if (i + 1) % 6 == 0:  # 每分钟打印一次进度
            print(f"等待中... ({i+1}/30)")
    
    print("❌ vLLM服务启动失败，请检查容器日志！")
    return False 
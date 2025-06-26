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
from typing import List, Dict, Any
from synapse_flow.db import get_pg_conn

# vLLM服务配置
VLLM_SERVICE = {
    "port": 8201,
    "container_name": "vllm"
}

class LevelAnalysisService:
    """层级分析服务"""
    
    def __init__(self, port: int = 8201):
        self.port = port
        self.base_url = f"http://localhost:{port}"
        self.confirmed_levels = []  # 存储已确认的层级信息
        self.level_path_stack = []  # 存储当前活跃的层级路径栈
    
    def call_vllm_api(self, messages: List[Dict[str, str]], max_tokens: int = 2000) -> str:
        """调用vLLM API"""
        url = f"{self.base_url}/v1/chat/completions"
        
        # 获取可用模型
        try:
            response = requests.get(f"{self.base_url}/v1/models", timeout=10)
            if response.status_code == 200:
                models_data = response.json()
                available_models = [model.get('id', '') for model in models_data.get('data', [])]
                lora_model_name = "llama3.1_8b"
                
                if lora_model_name in available_models:
                    model_name = lora_model_name
                elif available_models:
                    model_name = available_models[0]
                else:
                    model_name = "llama3.1_8b"
            else:
                model_name = "llama3.1_8b"
        except:
            model_name = "llama3.1_8b"
        
        payload = {
            "model": model_name,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": 0.0,
            "stream": False
        }
        
        try:
            response = requests.post(url, json=payload, timeout=300)
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"]
            else:
                print(f"API调用失败，状态码: {response.status_code}")
                return ""
        except Exception as e:
            print(f"API调用出错: {str(e)}")
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
            "index": len(self.confirmed_levels)  # 记录在confirmed_levels中的索引
        }
        
        # 更新层级路径栈
        # 1. 移除所有大于等于新层级的节点
        self.level_path_stack = [node for node in self.level_path_stack if node["level"] < new_level]
        
        # 2. 添加新节点
        self.level_path_stack.append(level_info)
        
        print(f"更新层级路径栈: {[node['level'] for node in self.level_path_stack]}")
    
    def get_context_path(self) -> List[Dict[str, Any]]:
        """
        获取当前活跃的层级路径
        返回递归向上路径中的节点
        注意：这里返回的是处理当前文本之前的路径，不包含当前文本
        """
        return self.level_path_stack.copy()
    
    def build_level_prompt(self, target_item: Dict[str, Any]) -> tuple:
        """构建层级判断的prompt"""
        system_prompt = """你是一个文本层级梳理专家，你的任务是**判断目标文本块的层级**

你会得到四种信息，辅助你确定目标文本块的层级：
1. 目标文本块的内容：这个文本块的内容是一个层级的开头部分，具有层级的内容与结构特征，可以方便你进行判断；
2. 层级标记：
（1）section level：这里指的从这里开始可能是一个结构层级，是整个文章的书写框架结构标题；
（2）context level：这里指的从这里开始可能是一个段落层级，是结构下文章段落内的叙述标题；
3. 目标文本块的前文相关递进层级：通过对比以往的层级，辅助你判断目标文本块作为层级开头应该属于什么层级；

**注意**：不管段落或结构层级的相同与否，并不能成为判断的决定性依据，要考虑内容与格式带来的影响。

回答格式：
如果是正常情况判断请回答：因为目标文本开头是前文XXX，且XXX，所以判断为{层级X}。
如果是特殊情况判断请回答：因为目标文本开头是前文XXX，且XXX，但我认为{层级主题错误/层级顺序混乱/层级格式不同却语义相关}，所以判断为{层级X}。

举例1：
我们已经确认：
{
  "text": "企业设立管理",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "第一节 登记信息确认",
  "isTitleMarked": section level,
}
是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且与上一级同为结构层级，所以判断为{层级二}。

举例2：
我们已经确认：
{
  "text": "中华人民共和国个人所得税法",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "第一条【征税范围】",
  "isTitleMarked": section level,
}
以上为第二层级开头内容格式；

请问：
{
  "text": "（一）因任职、受雇、履约等在中国境内提供劳务取得的所得；",
  "isTitleMarked": context level,
}
是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是上一级结构层级下的第一个段落层级，所以判断为{层级三}。

举例4：
我们已经确认：
{
  "text": "中华人民共和国增值税法大全",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "第一节 基本要素",
  "isTitleMarked": section level,
}
以上为第二层级开头内容格式；

{
  "text": "7.我们需要判断其具有的时间要素。...",
  "isTitleMarked": context level,
}
以上为第三层级开头内容格式；

{
  "text": "（1）时间的先后顺序：...",
  "isTitleMarked": context level,
}
是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且与上一级同为段落层级，所以判断为{层级四}。

举例5：
我们已经确认：
{
  "text": "企业设立管理",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "第一节 登记信息确认",
  "isTitleMarked": section level,
}
以上为第二层级开头内容格式；

{
  "text": "三、纳税人（扣缴义务人）身份信息报告",
  "isTitleMarked": section level,
}
以上为第三层级开头内容格式；

{
  "text": "5.从事生产、经营的纳税人，...",
  "isTitleMarked": context level,
}
以上为第四层级开头内容格式；

请问：
{
  "text": "第二节 增值税一般纳税人登记",
  "isTitleMarked": section level,
}
是第几层级的开头内容？

回答：因为目标文本开头是前文具有的一种旧层级格式，且是段落层级回到结构层级，所以判断为{层级二}。

举例6：（特殊）
我们已经确认：
{
  "text": "增值税法大全",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "第一节 基本要素",
  "isTitleMarked": section level,
}
以上为第二层级开头内容格式；

{
  "text": "7.我们需要判断其具有的时间要素。...",
  "isTitleMarked": context level,
}
以上为第三层级开头内容格式；

{
  "text": "（1）时间的先后顺序：...",
  "isTitleMarked": section level,
}
是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是段落层级回到结构层级，但我认为{层级主题错误}，所以判断为{层级四}。

举例7：（特殊）
我们已经确认：
{
  "text": "增值税法大全",
  "isTitleMarked": section level,
}
以上为第一层级开头内容格式；

{
  "text": "（5）我认为这本书是一本优秀的书籍，...",
  "isTitleMarked": context level,
}
以上为第二层级开头内容格式；

{
  "text": "第一节 基本要素",
  "isTitleMarked": section level,
}
是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是段落层级回到结构层级，但我认为{层级顺序混乱}，所以判断为{层级二}。"""
        
        # 构建递归向上路径的上下文信息
        context_info = ""
        context_path = self.get_context_path()
        
        if context_path:
            context_info = "我们已经确认：\n"
            for i, level_info in enumerate(context_path):
                context_info += f"{{\n"
                context_info += f'  "text": "{level_info["text"]}",\n'
                context_info += f'  "isTitleMarked": "{level_info["isTitleMarked"]}",\n'
                context_info += f"}}\n"
                context_info += f"以上为第{level_info['level']}层级开头内容格式；\n\n"
        
        # 构建目标文本块信息
        target_info = f"请问：\n{{\n"
        target_info += f'  "text": "{target_item["text"]}",\n'
        target_info += f'  "isTitleMarked": "{target_item["isTitleMarked"]}",\n'
        target_info += f"}}\n"
        target_info += "是第几层级的开头内容？"
        
        user_prompt = context_info + target_info
        
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
                    # 兼容旧格式
                    if "层级主题错误" in response:
                        result["special_type"] = "层级主题错误"
                    elif "顺序混乱" in response:
                        result["special_type"] = "顺序混乱"
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
            
            # 构建prompt
            system_prompt, user_prompt = self.build_level_prompt(item_data)
            
            # 构建消息
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            
            print(f"\n=== 处理数据 ===")
            print(f"目标文本: {item_data['text']}")
            print(f"标记类型: {item_data['isTitleMarked']}")
            print(f"当前层级路径栈: {[node['level'] for node in self.level_path_stack]}")
            
            # 调用API
            ai_response = self.call_vllm_api(messages)
            
            # 解析响应
            parsed_result = self.parse_level_response(ai_response)
            
            print(f"\n=== 输出结果 ===")
            print(f"AI Response: {ai_response}")
            print(f"解析结果: 层级{parsed_result['level']}")
            if parsed_result['is_special_case']:
                print(f"特殊情况: {parsed_result['special_type']}")
            print("=" * 50)
            
            # 如果成功解析到层级，更新层级路径栈和已确认层级列表
            if parsed_result['level'] is not None:
                # 更新层级路径栈
                self.update_level_path_stack(parsed_result['level'], item_data)
                
                # 添加到已确认层级列表
                confirmed_level_info = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result["level"],
                    "reasoning": parsed_result["reasoning"],
                    "is_special_case": parsed_result["is_special_case"],
                    "special_type": parsed_result["special_type"]
                }
                self.confirmed_levels.append(confirmed_level_info)
            
            # 返回处理结果
            return {
                "id": item_data.get("id"),
                "text": item_data["text"],
                "isTitleMarked": item_data["isTitleMarked"],
                "level": parsed_result["level"],
                "reasoning": parsed_result["reasoning"],
                "is_special_case": parsed_result["is_special_case"],
                "special_type": parsed_result["special_type"],
                "ai_response": ai_response
            }
            
        except Exception as e:
            print(f"处理单个数据项时出错: {str(e)}")
            return {
                "id": item_data.get("id"),
                "text": item_data["text"],
                "isTitleMarked": item_data["isTitleMarked"],
                "level": None,
                "reasoning": f"处理出错: {str(e)}",
                "is_special_case": False,
                "special_type": None,
                "ai_response": ""
            }
    
    def process_batch(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量处理数据列表"""
        results = []
        
        for item_data in data_list:
            result = self.process_single_item(item_data)
            results.append(result)
        
        return results
    
    def get_confirmed_levels(self) -> List[Dict[str, Any]]:
        """获取已确认的层级列表"""
        return self.confirmed_levels.copy()
    
    def get_level_sequence(self) -> List[int]:
        """获取层级序列"""
        return [level_info["level"] for level_info in self.confirmed_levels]

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
                "results": results
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
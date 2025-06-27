#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
vLLM层级判断处理器
用于处理需要上下文关系的层级判断任务
"""

import json
import time
import requests
import re
import datetime
from typing import List, Dict, Any

# vLLM服务配置
VLLM_SERVICE = {
    "port": 8201,
    "container_name": "vllm"
}

class LevelProcessor:
    """层级判断处理器"""
    
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
2. 层级标记：
（1）section level：这里指的从这里开始可能是一个结构层级，是整个文章的书写框架结构标题；
（2）context level：这里指的从这里开始可能是一个段落层级，是结构下文章段落内的叙述标题；
3. 目标文本块的前文相关递进层级：通过对比以往的层级，辅助你判断目标文本块作为层级开头应该属于什么层级；

**注意**：不管段落或结构层级的相同与否，并不能成为判断的决定性依据，要考虑内容与格式带来的影响。

回答格式：
如果是正常情况判断请回答：因为目标文本开头是前文XXX，且XXX，所以判断为{层级X}。
如果是特殊情况判断请回答：因为目标文本开头是前文XXX，且XXX，但我认为{层级主题错误/层级顺序混乱/层级格式不同却语义相关}，所以判断为{层级X}。

举例1：
我们已经确认：结构一层：企业设立管理（section level）
请问：结构二层：第一节 登记信息确认（section level）是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且与上一级同为结构层级，所以判断为{层级二}。

举例2：
我们已经确认：结构一层：中华人民共和国个人所得税法（section level）
结构二层：第一条【征税范围】（section level）
请问：段落三层：（一）因任职、受雇、履约等在中国境内提供劳务取得的所得；（context level）是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是上一级结构层级下的第一个段落层级，所以判断为{层级三}。

举例4：
我们已经确认：结构一层：中华人民共和国增值税法大全（section level）
结构二层：第一节 基本要素（section level）
段落三层：7.我们需要判断其具有的时间要素（context level）
请问：段落四层：（1）时间的先后顺序（context level）是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且与上一级同为段落层级，所以判断为{层级四}。

举例5：
我们已经确认：结构一层：企业设立管理（section level）
结构二层：第一节 登记信息确认（section level）
结构三层：三、纳税人（扣缴义务人）身份信息报告（section level）
段落四层：5.从事生产、经营的纳税人（context level）
请问：结构二层：第二节 增值税一般纳税人登记（section level）是第几层级的开头内容？

回答：因为目标文本开头是前文具有的一种旧层级格式，且是段落层级回到结构层级，所以判断为{层级二}。

举例6：（特殊）
我们已经确认：结构一层：增值税法大全（section level）
结构二层：第一节 基本要素（section level）
段落三层：7.我们需要判断其具有的时间要素（context level）
请问：结构四层：（1）时间的先后顺序（section level）是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是段落层级回到结构层级，但我认为{层级主题错误}，所以判断为{层级四}。

举例7：（特殊）
我们已经确认：结构一层：增值税法大全（section level）
段落二层：（5）我认为这本书是一本优秀的书籍（context level）
请问：结构二层：第一节 基本要素（section level）是第几层级的开头内容？

回答：因为目标文本开头为前文不具有的一种新层级格式，且是段落层级回到结构层级，但我认为{层级顺序混乱}，所以判断为{层级二}。"""
        
        # 构建递归向上路径的上下文信息
        context_info = ""
        context_path = self.get_context_path()
        
        if context_path:
            context_info = "我们已经确认："
            for i, level_info in enumerate(context_path):
                level_type = "结构" if level_info["isTitleMarked"] == "section level" else "段落"
                truncated_text = truncate_text(level_info["text"])
                context_info += f"{level_type}{level_info['level']}层：{truncated_text}（{level_info['isTitleMarked']}）"
                if i < len(context_path) - 1:
                    context_info += "，"
            context_info += "\n"
        
        # 构建目标文本块信息
        target_type = "结构" if target_item["isTitleMarked"] == "section level" else "段落"
        truncated_target_text = truncate_text(target_item["text"])
        target_info = f"{target_type}层级：{truncated_target_text}（{target_item['isTitleMarked']}）是第几层级的开头内容？"
        
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
                level_info = {
                    "text": item_data["text"],
                    "isTitleMarked": item_data["isTitleMarked"],
                    "level": parsed_result['level'],
                    "ai_response": ai_response
                }
                self.confirmed_levels.append(level_info)
            
            return {
                "original_data": item_data,
                "ai_response": ai_response,
                "parsed_result": parsed_result,
                "system_prompt": system_prompt,
                "user_prompt": user_prompt,
                "context_path": context_path_before  # 使用调用AI之前的路径
            }
            
        except Exception as e:
            print(f"处理数据时出错: {str(e)}")
            return {
                "original_data": item_data,
                "ai_response": "",
                "parsed_result": {"level": None, "reasoning": "", "is_special_case": False},
                "system_prompt": "",
                "user_prompt": "",
                "error": str(e)
            }
    
    def process_batch(self, data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量处理数据"""
        results = []
        
        for i, item_data in enumerate(data_list):
            print(f"处理第 {i+1}/{len(data_list)} 条数据...")
            result = self.process_single_item(item_data)
            results.append(result)
        
        return results
    
    def get_confirmed_levels(self) -> List[Dict[str, Any]]:
        """获取已确认的层级信息"""
        return self.confirmed_levels.copy()
    
    def get_level_sequence(self) -> List[int]:
        """获取层级序列"""
        return [level_info["level"] for level_info in self.confirmed_levels]

def load_data_from_json(file_path: str) -> List[Dict[str, Any]]:
    """从JSON文件加载数据"""
    encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312', 'latin1']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                data = json.load(f)
            print(f"成功加载数据，共 {len(data)} 条记录，使用编码: {encoding}")
            return data
        except UnicodeDecodeError:
            print(f"编码 {encoding} 失败，尝试下一个...")
            continue
        except Exception as e:
            print(f"使用编码 {encoding} 加载失败: {str(e)}")
            continue
    
    print(f"所有编码都失败了，无法加载数据")
    return []

def save_results_to_json(results: List[Dict[str, Any]], file_path: str):
    """保存结果到JSON文件"""
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        print(f"结果已保存到: {file_path}")
    except Exception as e:
        print(f"保存结果失败: {str(e)}")

def main():
    """主函数"""
    # 1. 初始化层级处理器
    processor = LevelProcessor(port=8201)
    
    # 2. 加载数据
    data = load_data_from_json("extracted_data.json")
    if not data:
        print("没有数据可处理")
        return
    
    # 3. 处理数据
    print("开始处理数据...")
    results = processor.process_batch(data)
    
    # 4. 保存结果
    output_file = f"level_analysis_results_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_results_to_json(results, output_file)
    
    # 5. 保存已确认的层级信息
    confirmed_levels = processor.get_confirmed_levels()
    levels_file = f"confirmed_levels_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    save_results_to_json(confirmed_levels, levels_file)
    
    print("层级判断处理完成！")
    print(f"共处理 {len(results)} 条数据")
    print(f"成功判断 {len(confirmed_levels)} 个层级")

if __name__ == "__main__":
    main() 
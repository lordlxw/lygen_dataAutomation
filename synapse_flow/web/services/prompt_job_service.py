# 针对指示词封装的service，凯铭用
from synapse_flow.db import get_pg_conn
import json
import time
import os
import requests
import subprocess
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from synapse_flow.web.services.dataset_job_service import query_pdf_text_contents, insert_pdf_text_contents
import re
import traceback
import datetime

# 配置参数
BASE_MODEL_PATH = "/data/training/model/Meta-Llama-3.1-8B-Instruct"
LORA_PATH = "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005"

# vLLM服务配置 - 使用8张显卡
VLLM_SERVICE = {
    "port": 8201,
    "container_name": "vllm"
}

def start_vllm_service():
    """启动vLLM服务"""
    print("开始启动vLLM服务...")
    
    # 检查服务是否已经运行
    try:
        response = requests.get(f"http://localhost:{VLLM_SERVICE['port']}/health", timeout=5)
        if response.status_code == 200:
            print(f"✅ vLLM服务已在端口 {VLLM_SERVICE['port']} 运行")
            return True
    except:
        pass
    
    # 停止可能存在的旧服务
    try:
        subprocess.run(["docker", "stop", VLLM_SERVICE["container_name"]], capture_output=True)
        # subprocess.run(["docker", "rm", VLLM_SERVICE["container_name"]], capture_output=True)
        print(f"✅ 停止旧服务 {VLLM_SERVICE['container_name']}")
    except:
        pass
    
    # 启动新的vLLM服务
    cmd = [
        "docker", "run",
        "--gpus", "all",
        "-v", "/data/.cache/vllm:/root/.cache/vllm",
        "-v", "/data/.cache/huggingface:/root/.cache/huggingface",
        "-v", "/data/training/model:/root/model",
        "-v", "/data/training/llama3.1_8b_checkpoint:/root/lora",
        f"-p", f"{VLLM_SERVICE['port']}:8000",
        "--ipc=host",
        "-d",
        f"--name", VLLM_SERVICE["container_name"],
        "vllm/vllm-openai:latest",
        "--enable-lora",
        f"--lora-modules", f"llama3.1_8b=/root/lora/20250604/checkpoint-1005",
        "--model", "/root/model/Meta-Llama-3.1-8B-Instruct",
        "--tensor-parallel-size", "8"
    ]
    
    try:
        print(f"启动vLLM服务，端口: {VLLM_SERVICE['port']}, 使用所有GPU")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        
        if result.returncode == 0:
            print(f"✅ vLLM服务启动成功，端口: {VLLM_SERVICE['port']}")
            # 等待服务启动
            time.sleep(30)
            return True
        else:
            print(f"❌ vLLM服务启动失败，端口: {VLLM_SERVICE['port']}, 错误: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ 启动vLLM服务出错，端口: {VLLM_SERVICE['port']}, 错误: {str(e)}")
        return False

def stop_vllm_service():
    """停止vLLM服务"""
    print("停止vLLM服务...")
    
    try:
        # 停止并删除容器
        subprocess.run(["docker", "stop", VLLM_SERVICE["container_name"]], capture_output=True)
        subprocess.run(["docker", "rm", VLLM_SERVICE["container_name"]], capture_output=True)
        print(f"✅ 停止vLLM服务，端口: {VLLM_SERVICE['port']}")
    except Exception as e:
        print(f"⚠️ 停止vLLM服务出错，端口: {VLLM_SERVICE['port']}, 错误: {str(e)}")

def check_vllm_service_health():
    """检查vLLM服务健康状态"""
    try:
        response = requests.get(f"http://localhost:{VLLM_SERVICE['port']}/health", timeout=10)
        return response.status_code == 200
    except:
        return False

def verify_lora_model_loaded():
    """验证LoRA模型是否正确加载"""
    try:
        print("验证LoRA模型加载状态...")
        response = requests.get(f"http://localhost:{VLLM_SERVICE['port']}/v1/models", timeout=10)
        
        if response.status_code == 200:
            models_data = response.json()
            available_models = [model.get('id', '') for model in models_data.get('data', [])]
            print(f"可用模型列表: {available_models}")
            
            lora_model_name = "llama3.1_8b"
            if lora_model_name in available_models:
                print(f"✅ LoRA模型 '{lora_model_name}' 已正确加载")
                return True
            else:
                print(f"❌ LoRA模型 '{lora_model_name}' 未找到")
                print(f"⚠️ 当前可用模型: {available_models}")
                return False
        else:
            print(f"❌ 无法获取模型列表，状态码: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 验证LoRA模型时出错: {str(e)}")
        return False

import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
import json

async def call_vllm_api_async(messages, max_tokens=2000, session=None):
    """异步调用vLLM API"""
    url = f"http://localhost:{VLLM_SERVICE['port']}/v1/chat/completions"
    
    # 首先获取可用的模型列表
    try:
        async with session.get(f"http://localhost:{VLLM_SERVICE['port']}/v1/models", timeout=10) as response:
            if response.status == 200:
                models_data = await response.json()
                available_models = [model.get('id', '') for model in models_data.get('data', [])]
                print(f"可用模型列表: {available_models}")
                
                # 优先使用 LoRA 模型
                lora_model_name = "llama3.1_8b"
                if lora_model_name in available_models:
                    model_name = lora_model_name
                    print(f"✅ 使用 LoRA 模型: {model_name}")
                elif available_models:
                    model_name = available_models[0]
                    print(f"⚠️ LoRA 模型未找到，使用第一个可用模型: {model_name}")
                else:
                    model_name = "llama3.1_8b"
                    print(f"⚠️ 未找到可用模型，使用默认模型: {model_name}")
            else:
                model_name = "llama3.1_8b"
                print(f"⚠️ 获取模型列表失败，使用默认模型: {model_name}")
    except Exception as e:
        model_name = "llama3.1_8b"
        print(f"⚠️ 获取模型列表出错: {str(e)}，使用默认模型: {model_name}")
    
    payload = {
        "model": model_name,
        "messages": messages,
        "max_tokens": max_tokens,
        "stream": False
    }
    
    try:
        async with session.post(url, json=payload, timeout=300) as response:
            if response.status == 200:
                result = await response.json()
                return result["choices"][0]["message"]["content"]
            else:
                error_text = await response.text()
                print(f"❌ API调用失败，状态码: {response.status}, 错误: {error_text[:200]}")
                return ""
    except Exception as e:
        print(f"❌ 异步API调用出错: {str(e)}")
        return ""

def call_vllm_api(messages, max_tokens=2000):
    """同步调用vLLM API（保持向后兼容）"""
    url = f"http://localhost:{VLLM_SERVICE['port']}/v1/chat/completions"
    
    # 首先获取可用的模型列表
    try:
        response = requests.get(f"http://localhost:{VLLM_SERVICE['port']}/v1/models", timeout=10)
        if response.status_code == 200:
            models_data = response.json()
            available_models = [model.get('id', '') for model in models_data.get('data', [])]
            print(f"可用模型列表: {available_models}")
            
            # 优先使用 LoRA 模型
            lora_model_name = "llama3.1_8b"
            if lora_model_name in available_models:
                model_name = lora_model_name
                print(f"✅ 使用 LoRA 模型: {model_name}")
            elif available_models:
                model_name = available_models[0]
                print(f"⚠️ LoRA 模型未找到，使用第一个可用模型: {model_name}")
            else:
                model_name = "llama3.1_8b"
                print(f"⚠️ 未找到可用模型，使用默认模型: {model_name}")
        else:
            model_name = "llama3.1_8b"
            print(f"⚠️ 获取模型列表失败，使用默认模型: {model_name}")
    except Exception as e:
        model_name = "llama3.1_8b"
        print(f"⚠️ 获取模型列表出错: {str(e)}，使用默认模型: {model_name}")
    
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
            print(f"❌ API调用失败，状态码: {response.status_code}")
            print(f"错误信息: {response.text[:200]}")  # 只显示前200个字符
            return ""
                
    except Exception as e:
        print(f"❌ API调用出错: {str(e)}")
        return ""

async def process_single_item(item_data, session, semaphore):
    """处理单个数据项的协程"""
    current_item = item_data["item"]
    context_data = item_data["context_data"]
    instruction = item_data["instruction"]
    original_index = item_data["index"]
    
    if not instruction:
        return {
            "index": original_index, "ai_response": "",
            "current_text": current_item.get("text", ""), "item": current_item
        }

    # 在任务内部构建 prompt 和 messages，确保数据隔离
    system_prompt, user_prompt = build_prompt(instruction, context_data)
    
    print(f"\n=== 处理数据 index: {original_index} ===")
    print(f"System Prompt: {system_prompt}")
    print(f"User Prompt: {user_prompt}")
    
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]

    async with semaphore:
        ai_response = await call_vllm_api_async(messages, session=session)
        return {
            "index": original_index, "ai_response": ai_response,
            "current_text": current_item.get("text", ""), "item": current_item
        }

async def process_batch_with_vllm_async(batch_data):
    """异步使用vLLM处理一批数据 (已修正)"""
    results = []

    # 创建信号量控制并发量 - 减少到64，避免8张显卡负载过重
    semaphore = asyncio.Semaphore(64)

    async with aiohttp.ClientSession() as session:
        # 修正之处：直接创建任务，并通过参数传递独立的数据 item_data
        tasks = [
            asyncio.create_task(process_single_item(item_data, session, semaphore))
            for item_data in batch_data
        ]
        
        print(f"开始并发处理 {len(tasks)} 个任务，并发限制: 64...")
        
        completed_tasks = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 按原始索引排序结果
        temp_results = []
        for i, result in enumerate(completed_tasks):
            if isinstance(result, Exception):
                print(f"处理数据时出错 (index: {batch_data[i]['index']}): {str(result)}")
                temp_results.append({
                    "index": batch_data[i]["index"],
                    "ai_response": "",
                    "current_text": batch_data[i]["item"].get("text", ""),
                    "item": batch_data[i]["item"]
                })
            else:
                original_index, ai_response = result
                # 打印输出结果
                print(f"\n=== 输出结果 index: {original_index} ===")
                print(f"AI Response: {ai_response}")
                print(f"Current Text: {batch_data[i]['item'].get('text', '')}")
                print("=" * 50)
                
                temp_results.append({
                    "index": original_index,
                    "ai_response": ai_response,
                    "current_text": batch_data[i]["item"].get("text", ""),
                    "item": batch_data[i]["item"]
                })
        
        # 按原始索引排序
        temp_results.sort(key=lambda x: x["index"])
        results.extend(temp_results)
    
    return results

def process_batch_with_vllm(batch_data):
    """使用vLLM处理一批数据（支持异步并发）"""
    # 使用异步处理
    try:
        return asyncio.run(process_batch_with_vllm_async(batch_data))
    except Exception as e:
        print(f"异步处理失败，回退到同步处理: {str(e)}")
        # 如果异步处理失败，回退到原来的同步处理
        return process_batch_with_vllm_sync(batch_data)

def process_batch_with_vllm_sync(batch_data):
    """同步使用vLLM处理一批数据（原来的实现）"""
    results = []
    
    for item_data in batch_data:
        try:
            current_item = item_data["item"]
            context_data = item_data["context_data"]
            instruction = item_data["instruction"]
            
            if not instruction:  # 非text类型，跳过处理
                results.append({
                    "index": item_data["index"],
                    "ai_response": "",
                    "current_text": current_item.get("text", ""),
                    "item": current_item
                })
                continue
            
            # 构建prompt
            system_prompt, user_prompt = build_prompt(instruction, context_data)
            
            # 打印prompt信息
            print(f"\n=== 处理数据 index: {item_data['index']} (同步模式) ===")
            print(f"System Prompt: {system_prompt}")
            print(f"User Prompt: {user_prompt}")
            # print(f"Context Data: {json.dumps(context_data, ensure_ascii=False, indent=2)}")
            
            # 构建消息格式
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            
            # 调用vLLM API
            ai_response = call_vllm_api(messages)
            
            # 打印输出结果
            print(f"\n=== 输出结果 index: {item_data['index']} (同步模式) ===")
            print(f"AI Response: {ai_response}")
            print(f"Current Text: {current_item.get('text', '')}")
            print("=" * 50)
            
            results.append({
                "index": item_data["index"],
                "ai_response": ai_response,
                "current_text": current_item.get("text", ""),
                "item": current_item
            })
            
        except Exception as e:
            print(f"处理数据时出错 (index: {item_data['index']}): {str(e)}")
            results.append({
                "index": item_data["index"],
                "ai_response": "",
                "current_text": current_item.get("text", ""),
                "item": current_item
            })
    
    return results

def build_prompt(instruction, input_data):
    """构建prompt，使用你的instruction模板"""
    # 修正前两个占位符的page_idx
    for idx, item in enumerate(input_data):
        if idx < 2 and item.get("text") == "(此text不是有效文本，不需要参与判断)" and item.get("page_idx", 0) == 0:
            item["page_idx"] = -1
    instruction_template = """
你是文本切割处理的审核专家，将会看到一个目标文本块，以及它的前两个文本块和后一个文本块。

你的任务是**目标文本块（即第三个）**（1）判断是哪一种新层级（2）是否存在以下四类错误，并给出对应判断和修改建议。
根据第三文本块上下文进行层级判断：
（1）结构新层级：第三文本块的开头是文章的结构性标题，特征如："第一章 增值税"等，是一个标题词汇；
（2）段落新层级：第三文本块的开头句子是正文段落内的叙述标题，特征也如：（1）、一、首先、a.等，但是一句句子；
（3）图表新层级：文本块开头句子正文内容中特指图表的小标题，特征如：图：XXXX，下表如：，等；**注意**不是内容里有说到图或表就是图表新层级，而是特指后面文本块跟着是表或图！
（4）附注新层级：文本块开头句子正文内容中特指附注的小标题，特征如："注：XX"等，**注意**不是内容里有提到是补充内容就是附注新层级，而是特指后面文本块跟着是附注内容！
（5）非新层级：文本块开头句子没有明确新文本层级结构特征；

错误文本内容判断：
（1）字符错误：文本含有不合理字符，如乱码、错误符号、混杂代码符号（公式不算）；
（2）格式错误：文本块开头是句子残段，其上半句存在于上一个文本块结尾；
（3）信息错误：文本块为空、为页码、目录、标题页、版权页、装订信息等与正文无关的内容。
（4）需要拆分：文本块有多个层级的文本块，在原文中加入"<mark>"将其区分。**注意**最后必须以<mark>作为结尾

正确文本内容判断：
如果目标文本块不存在上述四类问题，即为正常文本块。

判断顺序：（1）判断是哪一种新层级（2）判断文本块内容是否错误

输出格式要求：
判断结论请统一使用如下格式：
因为阅读上下文第三文本块XXX，所以判断为{XX新层级}。因为第三文本块因xxx，所以判断为{错误类型}，建议处理方式为：{修改方式}
如果文本无误，请回复：
因为阅读上下文第三文本块XXX，所以判断为{XX新层级}。因为第三文本块因没有四类错误，所以判断为{正确}。第三文本块不做任何修改。



举例情况（1）：开头是段落新层级且为字符错误；
    {
        "text": "答：可以。根据文件规定：",
        "page_idx": 6
    },
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用3%征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "，减按1%征收率@征收增值税。",
        "page_idx": 7
    },
你应该回复：因为阅读上下文第三文本块是正文段落内的叙述标题，所以判断为{段落新层级}。因为第三文本块因含有不合理字符，所以判断为{文本错误}。建议处理方式为：{二、增值税小规模纳税人适用3%征收率的应税销售收入。}

举例情况（2）：开头非新层级且格式错误（开头为残句）
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用3%征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "，减按1%征收率@征收增值税。",
        "page_idx": 7
    },
    {
        "text": "三、本公告执行至2027年12月31日。",
        "page_idx": 7
    },
你应该回复：因为阅读上下文第三文本块为这一层级的正文组成部分，所以判断为{非新层级}。因为第三文本块因开始处句子不完整，所以判断为{格式错误}。建议处理方式为：{向前合并}且{删除}

举例情况（3）：开头非新层级且需要拆分（包含多个层级）
[
  {
    "text": "（四）通过纳税客体的非转移进行的国际避税",
    "page_idx": 248
  },
  {
    "text": "纳税客体的非转移，又称物的不流动。物的不流动，是指跨国纳税人在不移动资金、货物和劳务的情况下，采取其他手段避免自己的所得受到税收管辖。",
    "page_idx": 248
  },
  {
    "text": "通过物的不流动进行国际避税，主要有两种做法：一是变更公司组织形式以改变所得性质；二是利用延期纳税方式。",
    "page_idx": 248
  },
  {
    "text": "1.变更公司组织形式以改变所得性质",
    "page_idx": 248
  }
]
你应该回复：因为阅读上下文第三文本块为这一层级的正文组成部分，所以判断为{非新层级}。因为第三文本块因含有多个文本块，所以判断为{需要拆分}。建议处理方式为：{通过物的不流动进行国际避税，主要有两种做法：<mark>一是变更公司组织形式以改变所得性质；<mark>二是利用延期纳税方式。<mark>}
**注意**如果后面的第四个文本块不会是新的层级，你需要在结尾标记上<mark>,从而告诉我们层级的结束位置！

举例情况（4）：开头非新层级且信息错误（无效内容）
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用3%征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "12366热点问答年度精选汇编",
        "page_idx": 7
    },
    {
        "text": "，减按1%征收率@征收增值税。",
        "page_idx": 7
    },
你应该回复：因为阅读上下文第三文本块为这一层级的正文组成部分，所以判断为{非新层级}。因为第三文本块因是夹杂信息，所以判断为{信息错误}。建议处理方式为：{删除}
"""
    
    input_text = json.dumps(input_data, ensure_ascii=False, indent=2)
    instruction_text = f"需要你回答的问题是：{instruction}"
    input_text = f"需要分析的这段语句如下：{input_text}"

    # 日志追加写入，带BOM，保证Windows记事本不乱码
    log_path = "prompt_input_log.txt"
    if not os.path.exists(log_path):
        with open(log_path, "w", encoding="utf-8-sig") as f:
            pass
    try:
        with open(log_path, "a", encoding="utf-8-sig") as logf:
            logf.write(f"\n[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]\n{input_text}\n")
    except Exception as e:
        print(f"[日志写入失败] {e}")

    prompt = f"{instruction_text}\n{input_text}\n请根据问题要求与问题进行回答。"
    
    return instruction_template, prompt

def get_api_key(key_name="openai"):
    """
    获取指定 key_name 的最新可用 API Key，默认是 'openai'

    Args:
        key_name (str): API key 的名称

    Returns:
        str or None: 找到则返回 API key 字符串，否则返回 None
    """
    try:
        conn = get_pg_conn()
        cursor = conn.cursor()

        sql = """
            SELECT api_key 
            FROM openapi_keys 
            WHERE status = 1 AND key_name = %s 
            ORDER BY updated_at DESC 
            LIMIT 1;
        """
        cursor.execute(sql, (key_name,))
        result = cursor.fetchone()

        if result:
            return result[0]
        else:
            print(f"⚠️ 未找到 key_name = '{key_name}' 的可用 API Key")
            return None

    except Exception as e:
        print(f"❌ 查询 API Key 出错: {e}")
        return None

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def split_text():
    print("split_text")
    return

def process_qa_for_version_0(run_id: str) -> dict:
    """
    对版本0的数据进行QA问答对处理，生成版本1 - 使用vLLM服务版本
    
    Args:
        run_id (str): 运行ID
        
    Returns:
        dict: 处理结果，包含新版本号和处理数量
    """
    try:
        print(f"开始处理QA问答对，run_id: {run_id}")
        
        # 1. 启动vLLM服务
        if not start_vllm_service():
            raise Exception("vLLM服务启动失败")
        
        # 等待服务启动完成
        print("等待vLLM服务启动...")
        max_wait_time = 300  # 最多等待5分钟
        check_interval = 10  # 每10秒检查一次
        waited_time = 0
        
        while waited_time < max_wait_time:
            if check_vllm_service_health():
                print(f"✅ vLLM服务健康检查通过，端口: {VLLM_SERVICE['port']}")
                break
            else:
                print(f"⏳ 服务尚未就绪，等待中... ({waited_time}/{max_wait_time}秒)")
                time.sleep(check_interval)
                waited_time += check_interval
        else:
            raise Exception(f"vLLM服务启动超时，等待了{max_wait_time}秒后仍未就绪，端口: {VLLM_SERVICE['port']}")
        
        # 验证LoRA模型是否正确加载
        if not verify_lora_model_loaded():
            print("⚠️ LoRA模型验证失败，但继续执行...")
        else:
            print("✅ LoRA模型验证成功")
        
        # 2. 获取版本0的数据
        version_0_data = query_pdf_text_contents(run_id, 0)
        
        if not version_0_data:
            raise Exception(f"未找到run_id {run_id} 的版本0数据")
        
        print(f"获取到版本0数据，共 {len(version_0_data)} 条记录")
        
        # 3. 按原始顺序处理所有数据
        data_to_process = []
        text_processed_count = 0
        
        # 按page_index和block_index排序，保持原始顺序
        sorted_data = sorted(version_0_data, key=lambda x: (x.get("page_index", 0), x.get("block_index", 0)))
        
        print(f"开始准备 {len(sorted_data)} 条数据，按页面和块索引排序")
        
        for i, current_item in enumerate(sorted_data):
            item_type = current_item.get("type", "正文")
            
            # 只处理text类型的文本块
            if item_type.lower() == "text" or item_type == "正文":
                text_processed_count += 1
                
                # 构建上下文数据（前两个和后一个文本块）
                context_data = []
                
                # 添加前两个text类型的文本块（如果存在）
                prev_count = 0
                for j in range(i-1, -1, -1):  # 从当前索引向前查找
                    if prev_count >= 2:  # 已经找到2个前文
                        break
                    prev_item = sorted_data[j]
                    prev_type = prev_item.get("type", "正文")
                    if prev_type.lower() == "text" or prev_type == "正文":
                        text = prev_item.get("text", "").strip()
                        if not text:  # 如果text为空
                            text = "(此text不是有效文本，不需要参与判断)"
                        context_data.insert(0, {  # 插入到开头，保持顺序
                            "text": text,
                            "page_idx": prev_item.get("page_index", 0)
                        })
                        prev_count += 1
                
                # 如果前文不足2个，用占位符填充
                while len(context_data) < 2:
                    context_data.insert(0, {
                        "text": "(此text不是有效文本，不需要参与判断)",
                        "page_idx": 0
                    })
                
                # 添加当前文本块（第三个）
                current_text = current_item.get("text", "").strip()
                if not current_text:  # 如果text为空
                    current_text = "(此text不是有效文本，不需要参与判断)"
                context_data.append({
                    "text": current_text,
                    "page_idx": current_item.get("page_index", 0)
                })
                
                # 添加后一个text类型的文本块（如果存在）
                has_next = False
                for j in range(i+1, len(sorted_data)):
                    next_item = sorted_data[j]
                    next_type = next_item.get("type", "正文")
                    if next_type.lower() == "text" or next_type == "正文":
                        next_text = next_item.get("text", "").strip()
                        if not next_text:  # 如果text为空
                            next_text = "(此text不是有效文本，不需要参与判断)"
                        context_data.append({
                            "text": next_text,
                            "page_idx": next_item.get("page_index", 0)
                        })
                        has_next = True
                        break  # 只添加第一个后文
                
                # 如果没有后文，添加占位符
                if not has_next:
                    context_data.append({
                        "text": "(此text不是有效文本，不需要参与判断)",
                        "page_idx": 0
                    })
                
                # 确保context_data正好有4个元素
                assert len(context_data) == 4, f"上下文数据长度不正确: {len(context_data)}, 应该是4个"
                
                # 构建instruction
                instruction = "请问第三文本块是否为新的层级？另外，内容是否正确，如果错误应该建议如何修改?"
                
                # 添加到处理队列
                data_to_process.append({
                    "index": i,
                    "item": current_item,
                    "context_data": context_data,
                    "instruction": instruction
                })
            else:
                # 对于非text类型（如table），添加到处理队列但标记为空处理
                data_to_process.append({
                    "index": i,
                    "item": current_item,
                    "context_data": [],
                    "instruction": ""
                })
        
        print(f"准备完成，共 {len(data_to_process)} 条数据待处理（其中 {text_processed_count} 条text类型）")
        
        # 4. 使用vLLM服务处理数据
        print("开始使用vLLM服务处理数据...")
        
        # 分批处理，避免单次请求过大
        batch_size = 5  # 每批处理5条数据，减少并发压力
        all_results = []
        
        for i in range(0, len(data_to_process), batch_size):
            batch = data_to_process[i:i+batch_size]
            print(f"处理批次 {i//batch_size + 1}/{(len(data_to_process) + batch_size - 1)//batch_size}，共 {len(batch)} 条数据")
            
            try:
                batch_results = process_batch_with_vllm(batch)
                all_results.extend(batch_results)
                print(f"✅ 批次 {i//batch_size + 1} 处理完成")
            except Exception as e:
                print(f"❌ 批次 {i//batch_size + 1} 处理失败: {str(e)}")
                # 对于失败的批次，添加空结果
                for item in batch:
                    all_results.append({
                        "index": item["index"],
                        "ai_response": "",
                        "current_text": item["item"].get("text", ""),
                        "item": item["item"]
                    })
        
        # 按原始索引排序
        all_results.sort(key=lambda x: x["index"])
        
        print(f"vLLM服务处理完成，共收集到 {len(all_results)} 条结果")
        
        # 5. 根据AI分析结果调整数据
        print("根据AI分析结果调整数据...")
        
        # 第一步：先解析所有AI建议，生成修正后的文本
        for i, analysis in enumerate(all_results):
            ai_response = analysis["ai_response"]
            current_text = analysis["current_text"]
            
            if ai_response:  # 有AI分析结果
                # 解析remark并调整数据
                adjusted_item = parse_remark_and_adjust_data(ai_response, current_text, i, all_results)
                # 将修正后的文本存储回结果中
                all_results[i]["adjusted_text"] = adjusted_item["text"]
                all_results[i]["level_type"] = adjusted_item["level_type"]
            else:
                # 没有AI分析结果，保持原文本
                all_results[i]["adjusted_text"] = current_text
                all_results[i]["level_type"] = 0
        
        # 第二步：处理向前合并的情况（使用修正后的文本）
        # 先收集所有需要向前合并的块，按索引排序
        merge_operations = []
        for i, analysis in enumerate(all_results):
            ai_response = analysis["ai_response"]
            if "{向前合并}" in ai_response and "{删除}" in ai_response:
                # 找到上一个非空文本
                prev_index = i - 1
                while prev_index >= 0 and not all_results[prev_index].get("adjusted_text", "").strip():
                    prev_index -= 1
                
                if prev_index >= 0:
                    merge_operations.append({
                        "current_index": i,
                        "target_index": prev_index,
                        "current_text": analysis.get("adjusted_text", "").lstrip() or analysis["item"].get("text", "").lstrip()
                    })
        
        # 按目标索引排序，确保合并顺序正确（从后往前合并）
        merge_operations.sort(key=lambda x: x["target_index"], reverse=True)
        
        # 执行合并操作
        for merge_op in merge_operations:
            current_index = merge_op["current_index"]
            target_index = merge_op["target_index"]
            current_text = merge_op["current_text"]
            
            # 获取目标文本块的内容
            target_text = all_results[target_index].get("adjusted_text", "").rstrip()
            
            # 检查目标文本是否包含<mark>标记
            if "<mark>" in target_text:
                # 如果包含<mark>标记，将被合并的文本插入到最后一个<mark>之前
                last_mark_pos = target_text.rfind("<mark>")
                if last_mark_pos != -1:
                    # 在最后一个<mark>之前插入被合并的文本
                    merged_text = target_text[:last_mark_pos] + current_text + target_text[last_mark_pos:]
                else:
                    # 异常情况，直接拼接
                    merged_text = target_text + current_text
            else:
                # 不包含<mark>标记，直接拼接
                merged_text = target_text + current_text
            
            # 正则清理<mark>后所有空白和换行
            merged_text = re.sub(r'<mark>[\s\u3000]*', '<mark>', merged_text)
            all_results[target_index]["adjusted_text"] = merged_text
            # 当前文本置空
            all_results[current_index]["adjusted_text"] = ""
            print(f"向前合并: 第{current_index}块文本合并到第{target_index}块，合并后文本: {merged_text[:100]}...")
        
        # 第三步：处理<mark>标记后的格式清理（移除层级调整逻辑）
        print("处理<mark>标记后的格式清理...")
        for i in range(len(all_results)):
            current_text = all_results[i].get("adjusted_text", "")
            
            # 清理<mark>\n格式问题
            if "<mark>\n" in current_text:
                current_text = current_text.replace("<mark>\n", "<mark>")
                all_results[i]["adjusted_text"] = current_text
                print(f"第{i}块文本清理<mark>\\n格式")
        
        # 第四步：构建最终的处理数据
        processed_data = []
        for i, analysis in enumerate(all_results):
            current_item = analysis["item"]
            ai_response = analysis["ai_response"]
            adjusted_text = analysis.get("adjusted_text", "")  # 使用最终调整后的文本
            level_type = analysis.get("level_type", 0)
            
            # 最终清理：确保所有文本都没有<mark>后空白和换行
            if adjusted_text:
                adjusted_text = re.sub(r'<mark>[ \t\r\n]*', '<mark>', adjusted_text)
            
            if ai_response:  # 有AI分析结果
                # 构建处理后的数据项
                processed_item = {
                    "text": adjusted_text,
                    "page_index": current_item.get("page_index", 0),
                    "text_level": current_item.get("text_level", 1),
                    "type": current_item.get("type", "正文"),
                    "block_index": current_item.get("block_index", 0),
                    "level_type": level_type,
                    "exclude_from_finetune": current_item.get("exclude_from_finetune", False),
                    "remark": ai_response,  # 将AI分析结果存储到remark字段
                    "original_text": current_item.get("text", "")  # 保存原始文本到original_text字段
                }
            else:
                # 没有AI分析结果（非text类型或处理失败）
                processed_item = {
                    "text": adjusted_text,  # 使用最终调整后的文本
                    "page_index": current_item.get("page_index", 0),
                    "text_level": current_item.get("text_level", 1),
                    "type": current_item.get("type", ""),
                    "block_index": current_item.get("block_index", 0),
                    "level_type": level_type,
                    "exclude_from_finetune": current_item.get("exclude_from_finetune", False),
                    "remark": "",  # 非text类型remark为空
                    "original_text": current_item.get("text", "")  # 保存原始文本到original_text字段
                }
            
            processed_data.append(processed_item)
        
        print(f"QA问答对处理完成，共处理 {len(processed_data)} 条记录（其中 {text_processed_count} 条text类型）")
        
        # 6. 保存为版本1，基于版本0
        new_version = insert_pdf_text_contents(run_id, processed_data, based_version=0)
        
        print(f"版本1数据保存成功，新版本号: {new_version}")
        
        return {
            "run_id": run_id,
            "new_version": new_version,
            "processed_count": len(processed_data),
            "text_processed_count": text_processed_count,
            "status": "success"
        }
        
    except Exception as e:
        print(f"QA问答对处理失败: {str(e)}")
        traceback.print_exc()
        raise
    finally:
        # 清理vLLM服务
        print("清理vLLM服务...")
        #stop_vllm_service()

def parse_remark_and_adjust_data(ai_response: str, current_text: str, current_index: int, all_data: list) -> dict:
    """
    解析AI返回的remark，并根据分析结果调整数据
    """
    # 默认值
    level_type = 0  # 默认非新层级
    adjusted_text = current_text

    try:
        # 层级类型判断
        if "判断为{结构新层级}" in ai_response:
            level_type = 1
        elif "判断为{段落新层级}" in ai_response:
            level_type = 2
        elif "判断为{图表新层级}" in ai_response:
            level_type = 3
        elif "判断为{附注新层级}" in ai_response:
            level_type = 4
        elif "判断为{非新层级}" in ai_response:
            level_type = 0
        # 兼容老的"新层级"写法
        elif "判断为{新层级}" in ai_response:
            level_type = 1

        # 2. 解析处理方式（原有逻辑不变）
        if "{删除}" in ai_response:
            adjusted_text = ""
        elif "{向前合并}" in ai_response and "{删除}" in ai_response:
            adjusted_text = current_text
        elif "{需要拆分}" in ai_response:
            pattern = r'建议处理方式为：\{(.*?)\}'
            match = re.search(pattern, ai_response, re.DOTALL)
            if match:
                suggested_text = match.group(1).strip()
                suggested_text = re.sub(r'\\\*\\\*', '**', suggested_text)
                suggested_text = re.sub(r'<mark>[\s\u3000]*', '<mark>', suggested_text)
                adjusted_text = suggested_text
        elif "{文本错误}" in ai_response:
            # 匹配两种格式：带冒号和不带冒号
            pattern1 = r'建议处理方式为：\{(.*?)\}'
            pattern2 = r'建议处理方式为\{(.*?)\}'
            
            match = re.search(pattern1, ai_response, re.DOTALL)
            if not match:
                match = re.search(pattern2, ai_response, re.DOTALL)
            
            if match:
                suggested_text = match.group(1).strip()
                suggested_text = re.sub(r'\\\*\\\*', '**', suggested_text)
                suggested_text = re.sub(r'<mark>[\s\u3000]*', '<mark>', suggested_text)
                adjusted_text = suggested_text
        else:
            adjusted_text = current_text

        if adjusted_text:
            adjusted_text = re.sub(r'<mark>[ \t\r\n]*', '<mark>', adjusted_text)

    except Exception as e:
        print(f"解析remark时出错: {str(e)}")
        level_type = 0
        adjusted_text = current_text

    return {
        "level_type": level_type,
        "text": adjusted_text
    }
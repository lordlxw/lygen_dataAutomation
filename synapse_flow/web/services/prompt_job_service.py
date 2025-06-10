# 针对指示词封装的service，凯铭用
from synapse_flow.db import get_pg_conn
import torch
import json
import time
import os
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
from synapse_flow.web.services.dataset_job_service import query_pdf_text_contents, insert_pdf_text_contents
import re
from multiprocessing import Process, Manager
from tqdm import tqdm
import traceback
import subprocess

# CUDA内存分配策略
os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# 使用8张卡
NUM_GPUS = 8
gpu_ids = [0, 1, 2, 3, 4, 5, 6, 7]

def clear_all_gpu_memory():
    """清理所有GPU的内存"""
    print("开始清理所有GPU内存...")
    for gpu_id in range(8):
        try:
            torch.cuda.set_device(gpu_id)
            torch.cuda.empty_cache()
            torch.cuda.synchronize()
            
            # 显示清理后的内存状态
            memory_info = torch.cuda.get_device_properties(gpu_id)
            total_memory = memory_info.total_memory / (1024**3)  # GB
            allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
            free_memory = total_memory - allocated
            
            print(f"GPU {gpu_id}: 清理后 - 总内存 {total_memory:.1f}GB, 已分配 {allocated:.1f}GB, 可用 {free_memory:.1f}GB")
            
        except Exception as e:
            print(f"清理GPU {gpu_id} 内存时出错: {e}")

def force_clear_gpu_memory(gpu_id):
    """强制清理指定GPU的内存"""
    try:
        torch.cuda.set_device(gpu_id)
        
        # 尝试释放所有缓存
        torch.cuda.empty_cache()
        torch.cuda.synchronize()
        
        # 如果内存仍然被占用，尝试重置设备
        allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
        if allocated > 10:  # 如果还有超过10GB被占用
            print(f"GPU {gpu_id} 内存仍被占用 {allocated:.1f}GB，尝试重置...")
            # 注意：这会中断当前在该GPU上的所有操作
            torch.cuda.reset_peak_memory_stats(gpu_id)
        
        print(f"GPU {gpu_id} 内存清理完成")
        
    except Exception as e:
        print(f"强制清理GPU {gpu_id} 内存时出错: {e}")

def system_clear_gpu_memory():
    """使用系统命令清理GPU内存（需要root权限）"""
    try:
        print("尝试使用系统命令清理GPU内存...")
        # 使用nvidia-smi重置GPU
        result = subprocess.run(['nvidia-smi', '--gpu-reset'], 
                              capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            print("✅ 系统级GPU内存清理成功")
        else:
            print(f"⚠️ 系统级GPU内存清理失败: {result.stderr}")
    except Exception as e:
        print(f"系统级GPU内存清理出错: {e}")

# 获取所有任务
def split_text():
        print("split_text")
        return


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

def build_prompt(instruction, input_data):
    """构建prompt，使用你的instruction模板"""
    
    instruction_template = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
你是文本切割处理的审核专家，将会看到一个目标文本块，以及它的前两个文本块和后一个文本块。

你的任务是**目标文本块（即第三个）**（1）是否为新层级（2）是否存在以下四类错误，并给出对应判断和修改建议。
文本块层级判断：
（1）新层级：文本块开头句子具有明确新文本层级结构特征如：（1）、一、首先、a.等；
（2）非新层级：文本块开头句子没有明确新文本层级结构特征；

错误文本内容判断：
（1）字符错误：文本含有不合理字符，如乱码、错误符号、混杂代码符号（公式不算）；
（2）格式错误：文本块开头是句子残段，其上半句存在于上一个文本块结尾；
（3）信息错误：文本块为空、为页码、目录、标题页、版权页、装订信息等与正文无关的内容。
（4）需要拆分：文本块有多个层级的文本块，在原文中加入"<mark>"将其区分。

正确文本内容判断：
如果目标文本块不存在上述四类问题，即为正常文本块。

判断顺序：（1）判断是否为新层级（2）判断文本块内容是否错误

输出格式要求：
判断结论请统一使用如下格式：
因为阅读上下文第三文本块XXX，所以判断为{是否为新层级}。因为第三文本块因xxx，所以判断为{错误类型}，建议处理方式为：{修改方式}
如果文本无误，请回复：
因为阅读上下文第三文本块XXX，所以判断为{是否为新层级}。因为第三文本块因没有四类错误，所以判断为{正确}。第三文本块不做任何修改。



举例情况（1）：开头是新层级且为字符错误；
    {
        "text": "答：可以。根据文件规定：",
        "page_idx": 6
    },
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用 $3 \\%$ 征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "，减按 $1 \\%$ 征收率@征收增值税。",
        "page_idx": 7
    },
你应该回复：因为阅读上下文第三文本块带有广义标题特征，所以判断为{新层级}。因为第三文本块因含有不合理字符，所以判断为{文本错误}。建议处理方式为：{二、增值税小规模纳税人适用 $3 \\%$ 征收率的应税销售收入}

举例情况（2）：开头非新层级且格式错误（开头为残句）
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用 $3 \\%$ 征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "，减按 $1 \\%$ 征收率@征收增值税。",
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
你应该回复：因为阅读上下文第三文本块为这一层级的正文组成部分，所以判断为{非新层级}。因为第三文本块因含有多个文本块，所以判断为{需要拆分}。建议处理方式为：{通过物的不流动进行国际避税，主要有两种做法：<mark>一是变更公司组织形式以改变所得性质；<mark>二是利用延期纳税方式。}

举例情况（4）：开头非新层级且信息错误（无效内容）
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、。增值税小规模纳税人适用 $3 \\%$ 征收率@的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "12366热点问答年度精选汇编",
        "page_idx": 7
    },
    {
        "text": "，减按 $1 \\%$ 征收率@征收增值税。",
        "page_idx": 7
    },
你应该回复：因为阅读上下文第三文本块为这一层级的正文组成部分，所以判断为{非新层级}。因为第三文本块因是夹杂信息，所以判断为{信息错误}。建议处理方式为：{删除}
<|eot_id|>
"""
    
    input_text = json.dumps(input_data, ensure_ascii=False, indent=2)
    instruction_text = f"需要你回答的问题是：{instruction}"
    input_text = f"需要分析的这段语句如下：{input_text}"
    prompt = f"{instruction_text}\n{input_text}\n请根据问题要求与问题进行回答。"
    
    return instruction_template, prompt

def process_batch(model, tokenizer, batch_data, gpu_id):
    """处理一批数据"""
    results = []
    
    for item_data in batch_data:
        try:
            current_item = item_data["item"]
            context_data = item_data["context_data"]
            instruction = item_data["instruction"]
            
            # 构建prompt
            system_prompt, user_prompt = build_prompt(instruction, context_data)
            
            # 构建消息格式
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            
            # 使用tokenizer应用chat模板
            text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            
            # 编码输入
            inputs = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=4096,
                padding=True
            ).to(f"cuda:{gpu_id}")
            
            # 生成回答
            with torch.no_grad():
                output = model.generate(
                    **inputs,
                    max_new_tokens=2000,
                    do_sample=False,
                    num_beams=1
                )
            
            # 解码输出
            decoded = tokenizer.decode(output[0], skip_special_tokens=True)
            ai_response = decoded.split("assistant")[-1].strip()
            
            # 清理内存
            del inputs, output
            torch.cuda.empty_cache()
            
            results.append({
                "index": item_data["index"],
                "ai_response": ai_response,
                "current_text": current_item.get("text", ""),
                "item": current_item
            })
            
        except Exception as e:
            print(f"GPU {gpu_id} 处理数据时出错 (index: {item_data['index']}): {str(e)}")
            results.append({
                "index": item_data["index"],
                "ai_response": "",
                "current_text": current_item.get("text", ""),
                "item": current_item
            })
    
    return results

def run_worker(gpu_id, data_to_process, return_dict):
    """GPU工作进程"""
    try:
        # 强制清理GPU内存
        force_clear_gpu_memory(gpu_id)
        
        torch.cuda.set_device(gpu_id)
        torch.cuda.empty_cache()
        
        # 检查GPU内存状态
        memory_info = torch.cuda.get_device_properties(gpu_id)
        total_memory = memory_info.total_memory / (1024**3)  # GB
        allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
        free_memory = total_memory - allocated
        
        print(f"GPU {gpu_id}: 总内存 {total_memory:.1f}GB, 已分配 {allocated:.1f}GB, 可用 {free_memory:.1f}GB")
        
        # 如果可用内存小于15GB，尝试强制清理
        if free_memory < 15:
            print(f"⚠️ GPU {gpu_id} 内存不足，尝试强制清理...")
            force_clear_gpu_memory(gpu_id)
            
            # 重新检查内存
            allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
            free_memory = total_memory - allocated
            print(f"GPU {gpu_id}: 强制清理后 - 可用 {free_memory:.1f}GB")
            
            # 如果仍然不足，跳过这个GPU
            if free_memory < 15:
                print(f"❌ GPU {gpu_id} 内存仍然不足，跳过处理")
                return_dict[gpu_id] = []
                return
        
        print(f"GPU {gpu_id}: 开始加载模型...")

        # 模型路径配置
        base_model_path = "/data/training/model/Meta-Llama-3.1-8B-Instruct"
        lora_path = "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005"

        # 加载tokenizer
        tokenizer = AutoTokenizer.from_pretrained(
            base_model_path,
            trust_remote_code=True
        )
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token

        # 加载基础模型，使用更保守的内存设置
        base_model = AutoModelForCausalLM.from_pretrained(
            base_model_path,
            device_map={'': gpu_id},
            torch_dtype=torch.bfloat16,
            trust_remote_code=True,
            low_cpu_mem_usage=True,
            max_memory={gpu_id: f"{int(free_memory * 0.7)}GB"}  # 只使用70%的可用内存
        ).eval()

        # 加载LoRA模型
        model = PeftModel.from_pretrained(
            base_model,
            model_id=lora_path,
            device_map={'': gpu_id}
        )
        model.eval()
        print(f"GPU {gpu_id}: 模型加载完成")

        # 获取分配给当前GPU的数据
        gpu_data = data_to_process[gpu_id::NUM_GPUS]
        total_items = len(gpu_data)
        print(f"GPU {gpu_id}: 开始处理 {total_items} 条数据")

        batch_size = 1
        results = []
        
        for i in range(0, total_items, batch_size):
            try:
                batch = gpu_data[i:i+batch_size]
                batch_results = process_batch(model, tokenizer, batch, gpu_id)
                results.extend(batch_results)
                
                progress = (i + batch_size) / total_items * 100
                print(f"GPU {gpu_id}: 进度 {progress:.2f}% ({i + batch_size}/{total_items})")
                
                # 每处理3个批次就清理一次内存
                if i % 3 == 0:
                    torch.cuda.empty_cache()
                    
            except torch.cuda.OutOfMemoryError as e:
                print(f"❌ GPU {gpu_id} 内存不足，停止处理: {e}")
                # 清理内存并继续处理剩余数据
                torch.cuda.empty_cache()
                continue
            except Exception as e:
                print(f"❌ GPU {gpu_id} 处理批次时出错: {e}")
                continue

        return_dict[gpu_id] = results
        print(f"✅ GPU {gpu_id} 完成任务，处理 {len(results)} 条")
        
    except Exception as e:
        print(f"❌ GPU {gpu_id} 执行出错：{str(e)}")
        traceback.print_exc()
        return_dict[gpu_id] = []
    finally:
        if 'model' in locals():
            del model
        if 'base_model' in locals():
            del base_model
        if 'tokenizer' in locals():
            del tokenizer
        torch.cuda.empty_cache()

def process_qa_for_version_0(run_id: str) -> dict:
    """
    对版本0的数据进行QA问答对处理，生成版本1 - 异步多GPU版本
    
    Args:
        run_id (str): 运行ID
        
    Returns:
        dict: 处理结果，包含新版本号和处理数量
    """
    try:
        print(f"开始处理QA问答对，run_id: {run_id}")
        
        # 启动前清理所有GPU内存
        print("启动前清理所有GPU内存...")
        clear_all_gpu_memory()
        
        # 如果GPU内存仍然被占用，尝试系统级清理
        print("检查GPU内存状态...")
        total_allocated = 0
        for gpu_id in range(8):
            try:
                torch.cuda.set_device(gpu_id)
                allocated = torch.cuda.memory_allocated(gpu_id) / (1024**3)  # GB
                total_allocated += allocated
            except:
                pass
        
        if total_allocated > 100:  # 如果总占用超过100GB
            print(f"GPU内存总占用 {total_allocated:.1f}GB，尝试系统级清理...")
            system_clear_gpu_memory()
            # 再次清理
            clear_all_gpu_memory()
        
        # 1. 获取版本0的数据
        version_0_data = query_pdf_text_contents(run_id, 0)
        
        if not version_0_data:
            raise Exception(f"未找到run_id {run_id} 的版本0数据")
        
        print(f"获取到版本0数据，共 {len(version_0_data)} 条记录")
        
        # 2. 按原始顺序处理所有数据，准备多GPU处理
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
                
                # 添加前两个文本块（如果存在）
                for j in range(max(0, i-2), i):
                    prev_item = sorted_data[j]
                    text = prev_item.get("text", "").strip()
                    if not text:  # 如果text为空
                        text = "(此text不是有效文本，不需要参与判断)"
                    context_data.append({
                        "text": text,
                        "page_idx": prev_item.get("page_index", 0)
                    })
                
                # 添加当前文本块（第三个）
                current_text = current_item.get("text", "").strip()
                if not current_text:  # 如果text为空
                    current_text = "(此text不是有效文本，不需要参与判断)"
                context_data.append({
                    "text": current_text,
                    "page_idx": current_item.get("page_index", 0)
                })
                
                # 添加后一个文本块（如果存在）
                if i + 1 < len(sorted_data):
                    next_item = sorted_data[i + 1]
                    next_text = next_item.get("text", "").strip()
                    if not next_text:  # 如果text为空
                        next_text = "(此text不是有效文本，不需要参与判断)"
                    context_data.append({
                        "text": next_text,
                        "page_idx": next_item.get("page_index", 0)
                    })
                
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
        
        # 3. 启动多GPU并行处理
        from torch.multiprocessing import set_start_method
        try:
            set_start_method('spawn', force=True)
        except RuntimeError:
            pass

        manager = Manager()
        return_dict = manager.dict()
        processes = []

        print(f"启动 {NUM_GPUS} 个GPU进程...")
        for idx, gpu_id in enumerate(gpu_ids):
            p = Process(target=run_worker, args=(gpu_id, data_to_process, return_dict))
            p.start()
            processes.append(p)
            time.sleep(2)  # 避免同时启动造成资源竞争

        # 等待所有进程完成
        for p in processes:
            p.join()

        # 4. 收集所有GPU的处理结果
        all_results = []
        for gpu_id in gpu_ids:
            if gpu_id in return_dict:
                all_results.extend(return_dict[gpu_id])
            else:
                print(f"⚠️ GPU {gpu_id} 没有返回结果，已跳过。")

        # 按原始索引排序
        all_results.sort(key=lambda x: x["index"])
        
        print(f"多GPU处理完成，共收集到 {len(all_results)} 条结果")
        
        # 5. 根据AI分析结果调整数据
        print("根据AI分析结果调整数据...")
        
        # 先处理向前合并的情况
        for i, analysis in enumerate(all_results):
            ai_response = analysis["ai_response"]
            if "{向前合并}" in ai_response and "{删除}" in ai_response:
                # 找到上一个非空文本
                prev_index = i - 1
                while prev_index >= 0 and not all_results[prev_index].get("current_text", "").strip():
                    prev_index -= 1
                
                if prev_index >= 0:
                    # 拼接文本到上一个数据项
                    prev_text = all_results[prev_index].get("current_text", "")
                    current_text = analysis["current_text"]
                    all_results[prev_index]["current_text"] = prev_text + current_text
                    # 当前文本置空
                    all_results[i]["current_text"] = ""
        
        # 然后构建最终的处理数据
        processed_data = []
        for i, analysis in enumerate(all_results):
            current_item = analysis["item"]
            ai_response = analysis["ai_response"]
            current_text = analysis["current_text"]  # 使用可能已经调整过的文本
            
            if ai_response:  # 有AI分析结果
                # 解析remark并调整数据
                adjusted_item = parse_remark_and_adjust_data(ai_response, current_text, i, all_results)
                
                # 构建处理后的数据项
                processed_item = {
                    "text": adjusted_item["text"],
                    "page_index": current_item.get("page_index", 0),
                    "text_level": current_item.get("text_level", 1),
                    "type": current_item.get("type", "正文"),
                    "block_index": current_item.get("block_index", 0),
                    "is_title_marked": adjusted_item["is_title_marked"],
                    "exclude_from_finetune": current_item.get("exclude_from_finetune", False),
                    "remark": ai_response,  # 将AI分析结果存储到remark字段
                    "original_text": current_item.get("text", "")  # 保存原始文本到original_text字段
                }
            else:
                # 没有AI分析结果（非text类型或处理失败）
                processed_item = {
                    "text": current_text,  # 使用可能已经调整过的文本
                    "page_index": current_item.get("page_index", 0),
                    "text_level": current_item.get("text_level", 1),
                    "type": current_item.get("type", ""),
                    "block_index": current_item.get("block_index", 0),
                    "is_title_marked": current_item.get("is_title_marked", False),
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
        raise

def parse_remark_and_adjust_data(ai_response: str, current_text: str, current_index: int, all_data: list) -> dict:
    """
    解析AI返回的remark，并根据分析结果调整数据
    
    Args:
        ai_response (str): AI返回的分析结果
        current_text (str): 当前文本内容（可能已经经过向前合并调整）
        current_index (int): 当前数据在列表中的索引
        all_data (list): 所有数据的列表
        
    Returns:
        dict: 调整后的数据项
    """
    # 默认值
    is_title_marked = False
    adjusted_text = current_text
    
    try:
        # 1. 解析层级判断
        if "{新层级}" in ai_response:
            is_title_marked = True
        elif "{非新层级}" in ai_response:
            is_title_marked = False
        
        # 2. 解析处理方式
        if "{删除}" in ai_response:
            # 删除：置空文本
            adjusted_text = ""
        elif "{需要拆分}" in ai_response:
            # 提取建议的处理方式
            pattern = r'建议处理方式为：\{(.*?)\}'
            match = re.search(pattern, ai_response, re.DOTALL)
            if match:
                suggested_text = match.group(1).strip()
                # 清理markdown标记，但保留<mark>标记
                suggested_text = re.sub(r'\\\*\\\*', '**', suggested_text)
                adjusted_text = suggested_text
        
        # 3. 其他情况（正确、格式错误等）保持原文本不变
        else:
            adjusted_text = current_text
            
    except Exception as e:
        print(f"解析remark时出错: {str(e)}")
        # 解析失败时保持原样
        is_title_marked = False
        adjusted_text = current_text
    
    return {
        "is_title_marked": is_title_marked,
        "text": adjusted_text
    }

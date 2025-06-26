import os
import json
import time
import subprocess
import threading
import shutil
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
import torch
from datasets import Dataset
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM, 
    DataCollatorForSeq2Seq, 
    TrainingArguments, 
    Trainer
)
from peft import LoraConfig, TaskType, get_peft_model

# LoRA训练配置
LORA_TRAINING_CONFIG = {
    "base_model_path": "/data/training/model/Meta-Llama-3.1-8B-Instruct",
    "lora_checkpoint_path": "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005",
    "checkpoint_base_dir": "/home/liuxinwei/checkpoint_data",
    "max_length": 3400,
    "per_device_train_batch_size": 2,
    "gradient_accumulation_steps": 4,
    "num_train_epochs": 3,
    "learning_rate": 1e-4,
    "save_steps": 100,
    "logging_steps": 10,
    "lora_rank": 16,
    "lora_alpha": 64,
    "lora_dropout": 0.05
}

# 全局变量存储训练任务状态
training_tasks = {}

def train_lora_model(training_data: Dict[str, Any], user_id: str = None) -> Dict[str, Any]:
    """
    执行LoRA模型训练
    
    Args:
        training_data: 训练数据，包含CSV文件路径、训练参数等
        user_id: 用户ID，用于记录训练任务
    
    Returns:
        Dict包含训练结果信息
    """
    try:
        print("开始LoRA模型训练...")
        
        # 生成训练任务ID
        task_id = f"lora_training_{int(time.time())}"
        
        # 记录训练任务到内存
        training_tasks[task_id] = {
            "task_id": task_id,
            "user_id": user_id,
            "status": "started",
            "create_time": datetime.now().isoformat(),
            "update_time": datetime.now().isoformat(),
            "error_message": None,
            "output_path": None
        }
        
        # 准备训练环境
        training_env = prepare_training_environment(training_data, task_id)
        
        # 更新输出路径
        training_tasks[task_id]["output_path"] = training_env["output_dir"]
        
        # 启动训练进程（异步）
        training_thread = threading.Thread(
            target=run_training_process,
            args=(training_env, task_id)
        )
        training_thread.daemon = True
        training_thread.start()
        
        # 返回任务信息
        return {
            "task_id": task_id,
            "status": "started",
            "message": "LoRA训练任务已启动",
            "start_time": datetime.now().isoformat(),
            "output_dir": training_env["output_dir"]
        }
        
    except Exception as e:
        print(f"❌ LoRA训练启动失败: {str(e)}")
        return {
            "status": "error",
            "message": f"训练启动失败: {str(e)}"
        }

def prepare_training_environment(training_data: Dict[str, Any], task_id: str) -> Dict[str, Any]:
    """
    准备训练环境
    
    Args:
        training_data: 训练数据
        task_id: 任务ID
    
    Returns:
        Dict: 训练环境配置
    """
    try:
        # 获取当前日期作为目录名
        current_date = datetime.now().strftime("%Y%m%d")
        
        # 创建输出目录
        output_dir = os.path.join(LORA_TRAINING_CONFIG["checkpoint_base_dir"], current_date)
        os.makedirs(output_dir, exist_ok=True)
        
        # 处理CSV文件
        csv_file_path = training_data.get("csv_file_path")
        if not csv_file_path or not os.path.exists(csv_file_path):
            raise Exception("CSV文件路径不存在")
        
        # 复制CSV文件到输出目录
        training_csv_path = os.path.join(output_dir, "training_data.csv")
        shutil.copy2(csv_file_path, training_csv_path)
        
        # 验证CSV文件格式
        validate_csv_file(training_csv_path)
        
        # 构建训练环境配置
        training_env = {
            "task_id": task_id,
            "output_dir": output_dir,
            "training_csv_path": training_csv_path,
            "base_model_path": LORA_TRAINING_CONFIG["base_model_path"],
            "max_length": LORA_TRAINING_CONFIG["max_length"],
            "per_device_train_batch_size": training_data.get("batch_size", LORA_TRAINING_CONFIG["per_device_train_batch_size"]),
            "gradient_accumulation_steps": LORA_TRAINING_CONFIG["gradient_accumulation_steps"],
            "num_train_epochs": training_data.get("num_epochs", LORA_TRAINING_CONFIG["num_train_epochs"]),
            "learning_rate": training_data.get("learning_rate", LORA_TRAINING_CONFIG["learning_rate"]),
            "save_steps": LORA_TRAINING_CONFIG["save_steps"],
            "logging_steps": LORA_TRAINING_CONFIG["logging_steps"],
            "lora_rank": LORA_TRAINING_CONFIG["lora_rank"],
            "lora_alpha": LORA_TRAINING_CONFIG["lora_alpha"],
            "lora_dropout": LORA_TRAINING_CONFIG["lora_dropout"]
        }
        
        print(f"✅ 训练环境准备完成: {output_dir}")
        return training_env
        
    except Exception as e:
        print(f"❌ 准备训练环境失败: {str(e)}")
        raise e

def validate_csv_file(csv_path: str):
    """
    验证CSV文件格式
    
    Args:
        csv_path: CSV文件路径
    """
    try:
        df = pd.read_csv(csv_path)
        required_columns = ["input", "output", "instruction"]
        
        for col in required_columns:
            if col not in df.columns:
                raise Exception(f"CSV文件缺少必需列: {col}")
        
        # 过滤空行
        df = df[df['input'].notna() & df['input'].str.strip().ne('')]
        
        if len(df) == 0:
            raise Exception("CSV文件中没有有效数据")
        
        print(f"✅ CSV文件验证通过，共 {len(df)} 条有效数据")
        
    except Exception as e:
        print(f"❌ CSV文件验证失败: {str(e)}")
        raise e

def run_training_process(training_env: Dict[str, Any], task_id: str):
    """
    运行训练进程
    
    Args:
        training_env: 训练环境配置
        task_id: 任务ID
    """
    try:
        print(f"开始执行训练任务: {task_id}")
        
        # 更新任务状态为运行中
        update_training_status(task_id, "running")
        
        # 执行训练
        result = execute_training(training_env)
        
        if result["success"]:
            print(f"✅ 训练任务 {task_id} 完成")
            update_training_status(task_id, "completed", output_path=training_env["output_dir"])
        else:
            print(f"❌ 训练任务 {task_id} 失败: {result['error']}")
            update_training_status(task_id, "failed", result['error'])
            
    except Exception as e:
        print(f"❌ 训练任务 {task_id} 出错: {str(e)}")
        update_training_status(task_id, "error", str(e))

def execute_training(training_env: Dict[str, Any]) -> Dict[str, Any]:
    """
    执行训练
    
    Args:
        training_env: 训练环境配置
    
    Returns:
        Dict: 训练结果
    """
    try:
        # 设置GPU
        os.environ["CUDA_VISIBLE_DEVICES"] = "0"
        
        # 1. 加载和处理数据
        print("正在加载数据集...")
        ds = load_and_process_data(training_env["training_csv_path"])
        
        # 2. 设置tokenizer
        print("正在加载tokenizer...")
        tokenizer = setup_tokenizer(training_env["base_model_path"])
        
        # 3. 处理数据
        print("正在处理数据...")
        tokenized_dataset = ds['train'].map(
            lambda example, index: process_func(example, index, ds['train'], tokenizer, training_env["max_length"]), 
            with_indices=True
        )
        
        # 4. 设置模型和LoRA
        print("正在设置模型和LoRA...")
        model = setup_model_and_lora(training_env["base_model_path"], training_env)
        
        # 5. 训练模型
        print("开始训练模型...")
        train_model(model, tokenizer, tokenized_dataset, training_env)
        
        return {"success": True}
        
    except Exception as e:
        print(f"❌ 训练执行失败: {str(e)}")
        return {"success": False, "error": str(e)}

def load_and_process_data(csv_file_path: str) -> Dataset:
    """加载并处理CSV数据集"""
    print("正在加载数据集...")
    ds = Dataset.from_csv(csv_file_path)
    ds = ds.filter(lambda x: x['input'] and str(x['input']).strip())  # 去掉空的行
    print(f"数据集加载完成，共{len(ds)}条数据")
    return ds

def setup_tokenizer(model_path: str):
    """设置tokenizer"""
    print("正在加载tokenizer...")
    tokenizer = AutoTokenizer.from_pretrained(
        model_path, 
        use_fast=False, 
        trust_remote_code=True, 
        local_files_only=True
    )
    tokenizer.pad_token = tokenizer.eos_token
    return tokenizer

def process_func(example, index, ds, tokenizer, max_length):
    """数据处理函数"""
    # 构建各部分的输入
    input_text = f"需要分析的这段语句是：\"{example['input']}\""
    output_text = example['output']
    
    # 使用原始的 instruction 模板
    instruction_text = """<|begin_of_text|><|start_header_id|>system<|end_header_id|>
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
**注意**如果后面的第四个文本块不会是新的层级，你需要在结尾标记上<mark>,从而告诉我们层级的结束位置！

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
    
    # 构建完整的instruction文本
    user_part = f"""<|start_header_id|>user<|end_header_id|>
    
    需要你回答的问题是："{example['instruction']}"
     {input_text}
    请根据问题要求与问题进行回答<|eot_id|><|start_header_id|>assistant<|end_header_id|>
    """
    
    instruction_text = instruction_text + user_part

    # Tokenize instruction with response
    instruction = tokenizer(instruction_text, add_special_tokens=False)
    response = tokenizer(f"{output_text}<|eot_id|>", add_special_tokens=False)
    input_ids = instruction["input_ids"] + response["input_ids"] + [tokenizer.pad_token_id]
    attention_mask = instruction["attention_mask"] + response["attention_mask"] + [1]
    labels = [-100] * len(instruction["input_ids"]) + response["input_ids"] + [tokenizer.pad_token_id]

    # 确保长度不超过max_length
    if len(input_ids) > max_length:
        input_ids = input_ids[:max_length]
        attention_mask = attention_mask[:max_length]
        labels = labels[:max_length]

    return {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "labels": labels
    }

def setup_model_and_lora(model_path: str, training_env: Dict[str, Any]):
    """设置模型和LoRA配置"""
    print("正在加载基础模型...")
    model = AutoModelForCausalLM.from_pretrained(
        model_path, 
        device_map="auto",
        torch_dtype=torch.bfloat16
    )
    
    # 通过梯度检查点进行内存优化
    model.enable_input_require_grads()
    
    # LoRA的配置参数
    config = LoraConfig(
        task_type=TaskType.CAUSAL_LM, 
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        inference_mode=False,  # 训练模式
        r=training_env["lora_rank"],  # Lora 秩
        lora_alpha=training_env["lora_alpha"],  # Lora alpha
        lora_dropout=training_env["lora_dropout"]  # Dropout 比例
    )
    
    model = get_peft_model(model, config)
    print("可训练参数占比:", model.print_trainable_parameters())
    
    return model

def train_model(model, tokenizer, tokenized_dataset, training_env: Dict[str, Any]):
    """训练模型"""
    print("开始训练模型...")
    
    args = TrainingArguments(
        output_dir=training_env["output_dir"],
        per_device_train_batch_size=training_env["per_device_train_batch_size"],
        gradient_accumulation_steps=training_env["gradient_accumulation_steps"],
        logging_steps=training_env["logging_steps"],
        num_train_epochs=training_env["num_train_epochs"],
        save_steps=training_env["save_steps"],
        learning_rate=training_env["learning_rate"],
        save_on_each_node=True,
        gradient_checkpointing=True
    )

    trainer = Trainer(
        model=model,
        args=args,
        train_dataset=tokenized_dataset,
        data_collator=DataCollatorForSeq2Seq(tokenizer=tokenizer, padding=True),
    )

    trainer.train()
    print("训练完成！")

def update_training_status(task_id: str, status: str, error_message: str = None, output_path: str = None):
    """
    更新训练任务状态（内存存储）
    
    Args:
        task_id: 任务ID
        status: 状态
        error_message: 错误信息
        output_path: 输出路径
    """
    if task_id in training_tasks:
        training_tasks[task_id]["status"] = status
        training_tasks[task_id]["update_time"] = datetime.now().isoformat()
        if error_message:
            training_tasks[task_id]["error_message"] = error_message
        if output_path:
            training_tasks[task_id]["output_path"] = output_path
        print(f"✅ 更新任务状态: {task_id} -> {status}")

def get_training_status(task_id: str) -> Dict[str, Any]:
    """
    获取训练任务状态（内存存储）
    
    Args:
        task_id: 任务ID
    
    Returns:
        Dict: 训练任务状态信息
    """
    if task_id in training_tasks:
        return training_tasks[task_id]
    else:
        return {"error": "任务不存在"}

def list_training_tasks(user_id: str = None, limit: int = 20) -> list:
    """
    列出训练任务（内存存储）
    
    Args:
        user_id: 用户ID（可选）
        limit: 限制数量
    
    Returns:
        list: 训练任务列表
    """
    tasks = list(training_tasks.values())
    
    # 按用户ID过滤
    if user_id:
        tasks = [task for task in tasks if task.get("user_id") == user_id]
    
    # 按创建时间排序（最新的在前）
    tasks.sort(key=lambda x: x.get("create_time", ""), reverse=True)
    
    # 限制数量
    return tasks[:limit] 
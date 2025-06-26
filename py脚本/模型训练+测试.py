import os
import torch
import pandas as pd
from datasets import load_dataset, Dataset
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM, 
    DataCollatorForSeq2Seq, 
    TrainingArguments, 
    Trainer, 
    GenerationConfig
)
from peft import LoraConfig, TaskType, get_peft_model, PeftModel

# 设置GPU
os.environ["CUDA_VISIBLE_DEVICES"] = "0"

def load_and_process_data(csv_file_path):
    """加载并处理CSV数据集"""
    print("正在加载数据集...")
    ds = load_dataset("csv", data_files=csv_file_path)
    ds = ds.filter(lambda x: x['input'])  # 去掉空的行
    print(f"数据集加载完成，共{len(ds['train'])}条数据")
    return ds

def setup_tokenizer(model_path):
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

def process_func(example, index, ds):
    """数据处理函数"""
    MAX_LENGTH = 3400  # 根据需要调整最大长度

    # 构建各部分的输入
    input_text = f"需要分析的这段语句是："{example['input']}""
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
    """ + f"""<|start_header_id|>user<|end_header_id|>
    
    需要你回答的问题是："{example['instruction']}"
     {input_text}
    请根据问题要求与问题进行回答<|eot_id|><|start_header_id|>assistant<|end_header_id|>
    """

    # Tokenize instruction with response
    instruction = tokenizer(instruction_text, add_special_tokens=False)
    response = tokenizer(f"{output_text}<|eot_id|>", add_special_tokens=False)
    input_ids = instruction["input_ids"] + response["input_ids"] + [tokenizer.pad_token_id]
    attention_mask = instruction["attention_mask"] + response["attention_mask"] + [1]
    labels = [-100] * len(instruction["input_ids"]) + response["input_ids"] + [tokenizer.pad_token_id]

    # 确保长度不超过MAX_LENGTH
    if len(input_ids) > MAX_LENGTH:
        input_ids = input_ids[:MAX_LENGTH]
        attention_mask = attention_mask[:MAX_LENGTH]
        labels = labels[:MAX_LENGTH]

    return {
        "input_ids": input_ids,
        "attention_mask": attention_mask,
        "labels": labels
    }

def setup_model_and_lora(model_path):
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
        r=16,  # Lora 秩
        lora_alpha=64,  # Lora alpha，具体作用参见 Lora 原理
        lora_dropout=0.05  # Dropout 比例
    )
    
    model = get_peft_model(model, config)
    print("可训练参数占比:", model.print_trainable_parameters())
    
    return model

def train_model(model, tokenizer, tokenized_dataset, output_dir):
    """训练模型"""
    print("开始训练模型...")
    
    args = TrainingArguments(
        output_dir=output_dir,
        per_device_train_batch_size=2,
        gradient_accumulation_steps=4,
        logging_steps=10,
        num_train_epochs=3,
        save_steps=100,
        learning_rate=1e-4,
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

def load_trained_model(base_model_path, lora_path):
    """加载训练好的模型"""
    print("正在加载训练好的模型...")
    
    # 加载tokenizer
    tokenizer = AutoTokenizer.from_pretrained(base_model_path, trust_remote_code=True)

    # 加载模型
    model = AutoModelForCausalLM.from_pretrained(
        base_model_path, 
        device_map="auto",
        torch_dtype=torch.bfloat16, 
        trust_remote_code=True
    ).eval()

    # 加载lora权重
    model = PeftModel.from_pretrained(model, model_id=lora_path)
    
    return model, tokenizer

def generate_response(model, tokenizer, example, instruction_template):
    """生成响应"""
    instruction_text = f"需要你回答的问题是："{example['instruction']}""
    input_text = f"需要分析的这段语句如下："{example['input']}""
    
    prompt = f"{instruction_text}\n{input_text}\n请根据问题要求与问题进行回答。"
    
    # 转换为模型输入格式
    messages = [
        {"role": "system", "content": instruction_template},
        {"role": "user", "content": prompt}
    ]
    
    input_ids = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    model_inputs = tokenizer([input_ids], return_tensors="pt").to('cuda')

    # 生成响应
    generated_ids = model.generate(model_inputs.input_ids, max_new_tokens=2000)
    generated_ids = [
        output_ids[len(input_ids):] for input_ids, output_ids in zip(model_inputs.input_ids, generated_ids)
    ]

    # 解码并保存响应
    response = tokenizer.batch_decode(generated_ids, skip_special_tokens=True)[0]
    
    return response

def inference_on_test_data(model, tokenizer, test_csv_path, output_csv_path):
    """在测试数据上进行推理"""
    print("开始推理...")
    
    # 加载测试数据
    ds = load_dataset("csv", data_files=test_csv_path)
    ds = ds['train'].filter(lambda x: 'input' in x and x['input'])
    
    # 定义instruction模板
    instruction_template = """
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
"""
    
    # 应用模型并生成output
    outputs = []
    last_5_outputs = []
    last_page = None
    
    for i, example in enumerate(ds):
        print(f"处理第 {i+1}/{len(ds)} 条数据...")
        
        # 使用instruction模板生成output
        output = generate_response(model, tokenizer, example, instruction_template, last_5_outputs, last_page)
        last_5_outputs.append(output)
        if len(last_5_outputs) > 5:
            last_5_outputs.pop(0)
            
        # 将生成的output与原始input一起保存
        outputs.append({
            'input': example['input'],
            'output': output,
        })

    # 保存到DataFrame和CSV
    df = pd.DataFrame(outputs)
    df.to_csv(output_csv_path, index=False, encoding='utf-8-sig')
    print(f"推理结果已保存到 {output_csv_path}")

def main():
    """主函数"""
    # 配置路径
  # ==================== 路径配置区域 ====================
    
    # 基础模型路径 - 预训练的Llama-3.1-8B-Instruct模型
    # 作用：这是我们要进行微调的基础大语言模型，包含了预训练好的权重
    base_model_path = '/data/training/model/Meta-Llama-3.1-8B-Instruct/'
    
    # 训练数据路径 - 包含人工标注的训练数据
    # 作用：CSV文件，包含input（输入文本）和output（人工标注的正确答案）
    # 格式：每行包含input、output、instruction等字段
    train_csv_path = '人工审核AI-20250616.csv'
    
    # 测试数据路径 - 用于推理测试的数据
    # 作用：包含需要模型进行审核的新文本数据，用于测试模型效果
    test_csv_path = '/home/kopy/policy-data/verify/results/texting20250609.csv'
    
    # 训练输出目录 - 保存训练过程中的模型检查点
    # 作用：训练过程中会定期保存模型权重到这个目录
    # 包含：checkpoint-xxx文件夹，每个文件夹包含一个训练阶段的模型
    output_dir = '/home/kopy/policy-data/verify/output-人工审核/llama3_1_instruct_lora/20250616'
    
    # LoRA权重路径 - 已经训练好的LoRA适配器权重
    # 作用：这是之前训练好的LoRA权重，可以直接加载用于推理
    # 注意：这个路径指向一个具体的checkpoint文件夹
    lora_path = '/home/kopy/policy-data/verify/output-人工审核/llama3_1_instruct_lora/20250608/checkpoint-1944'
    
    # 推理结果保存路径 - 模型推理结果的输出文件
    # 作用：保存模型对新数据的审核结果，包含input和生成的output
    result_csv_path = '/home/kopy/policy-data/verify/results/1-result-20250609.csv'
    
    # 步骤1: 加载和处理数据
    ds = load_and_process_data(train_csv_path)
    
    # 步骤2: 设置tokenizer
    tokenizer = setup_tokenizer(base_model_path)
    
    # 步骤3: 处理数据
    print("正在处理数据...")
    tokenized_id = ds['train'].map(
        lambda example, index: process_func(example, index, ds['train']), 
        with_indices=True
    )
    
    # 步骤4: 设置模型和LoRA
    model = setup_model_and_lora(base_model_path)
    
    # 步骤5: 训练模型 用一张卡
    train_model(model, tokenizer, tokenized_id, output_dir)
    
    # 步骤6: 加载训练好的模型进行推理 用八张卡
    trained_model, trained_tokenizer = load_trained_model(base_model_path, lora_path)
    
    # 步骤7: 在测试数据上进行推理
    inference_on_test_data(trained_model, trained_tokenizer, test_csv_path, result_csv_path)
    
    print("所有任务完成！")

if __name__ == "__main__":
    main() 
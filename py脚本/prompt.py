import pandas as pd
import torch
import os
import json
from transformers import AutoTokenizer, AutoModelForCausalLM
from peft import PeftModel
from multiprocessing import Process, Manager
from tqdm import tqdm
import time
import traceback

# CUDA内存分配策略
os.environ['PYTORCH_CUDA_ALLOC_CONF'] = 'expandable_segments:True'

# 路径配置
base_model_path = "/data/training/model/Meta-Llama-3.1-8B-Instruct"
lora_path = "/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005"
input_csv = "/home/liuxinwei/Csv/20250606/20.csv"
final_output_path = "/home/liuxinwei/Csv/20250606/303030.csv"

# 只用前4张卡
NUM_GPUS = 4
gpu_ids = [0, 1, 2, 3]

# instruction模板
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
（4）需要拆分：文本块有多个层级的文本块，在原文中加入“<mark>”将其区分。

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
def build_prompt(example):
    # example 是 Series
    input_data = json.loads(example['input'])
    input_text = json.dumps(input_data, ensure_ascii=False, indent=2)
    instruction_text = f"需要你回答的问题是：{example['instruction']}"
    input_text = f"需要分析的这段语句如下：{input_text}"
    prompt = f"{instruction_text}\n{input_text}\n请根据问题要求与问题进行回答。"
    return instruction_template, prompt

def process_batch(model, tokenizer, batch, gpu_id):
    results = []
    for _, example in batch.iterrows():
        try:
            system_prompt, user_prompt = build_prompt(example)
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ]
            text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
            inputs = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=4096,
                padding=True
            ).to(f"cuda:{gpu_id}")

            with torch.no_grad():
                output = model.generate(
                    **inputs,
                    max_new_tokens=2000,
                    do_sample=False,
                    num_beams=1
                )
            decoded = tokenizer.decode(output[0], skip_special_tokens=True)
            response = decoded.split("assistant")[-1].strip()
            results.append({
                "index": int(example['index']),
                "instruction": example['instruction'],
                "input": example['input'],
                "output": response
            })
            del inputs, output
            torch.cuda.empty_cache()
        except Exception as e:
            print(f"处理数据时出错 (index: {example['index']}): {str(e)}")
            results.append({
                "index": int(example['index']),
                "instruction": example['instruction'],
                "input": example['input'],
                "output": f"处理出错: {str(e)}"
            })
    return results

def run_worker(gpu_id, return_dict):
    try:
        torch.cuda.set_device(gpu_id)
        torch.cuda.empty_cache()
        print(f"GPU {gpu_id}: 开始加载模型...")

        tokenizer = AutoTokenizer.from_pretrained(
            base_model_path,
            trust_remote_code=True
        )
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token

        base_model = AutoModelForCausalLM.from_pretrained(
            base_model_path,
            device_map={'': gpu_id},
            torch_dtype=torch.bfloat16,
            trust_remote_code=True,
            low_cpu_mem_usage=True
        ).eval()

        model = PeftModel.from_pretrained(
            base_model,
            model_id=lora_path,
            device_map={'': gpu_id}
        )
        model.eval()
        print(f"GPU {gpu_id}: 模型加载完成")

        df = pd.read_csv(input_csv)
        df = df[df['input'].notnull() & df['instruction'].notnull()].reset_index(drop=True)
        df['index'] = df.index  # 新增index列

        gpu_data = df.iloc[gpu_id::NUM_GPUS].copy()
        total_items = len(gpu_data)
        print(f"GPU {gpu_id}: 开始处理 {total_items} 条数据")

        batch_size = 1
        results = []
        for i in range(0, total_items, batch_size):
            batch = gpu_data.iloc[i:i+batch_size]
            batch_results = process_batch(model, tokenizer, batch, gpu_id)
            results.extend(batch_results)
            progress = (i + batch_size) / total_items * 100
            print(f"GPU {gpu_id}: 进度 {progress:.2f}% ({i + batch_size}/{total_items})")
            if i % 10 == 0:
                torch.cuda.empty_cache()

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
        torch.cuda.empty_cache()

def main():
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
        p = Process(target=run_worker, args=(gpu_id, return_dict))
        p.start()
        processes.append(p)
        time.sleep(2)

    for p in processes:
        p.join()

    all_results = []
    for gpu_id in gpu_ids:
        if gpu_id in return_dict:
            all_results.extend(return_dict[gpu_id])
        else:
            print(f"⚠️ GPU {gpu_id} 没有返回结果，已跳过。")

    if all_results:
        df_out = pd.DataFrame(all_results).sort_values("index")
        df_out.to_csv(final_output_path, index=False, encoding="utf-8-sig")
        print(f"✅ 所有可用 GPU 推理完成，已保存至：{final_output_path}")
    else:
        print("❌ 所有进程失败，没有结果保存。")

if __name__ == "__main__":
    main()
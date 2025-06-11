# QA问答对处理服务使用说明

## 概述

本服务用于对版本0的PDF文本数据进行QA问答对处理，生成版本1数据。服务使用本地训练的专业文本切割处理审核模型，对文本块进行层级判断和错误检测。

## 功能特性

- **本地模型调用**: 使用训练好的本地模型，无需外部API
- **专业文本审核**: 基于专业文本切割处理审核模型
- **上下文分析**: 结合前后文进行智能判断
- **自动错误检测**: 检测字符错误、格式错误、信息错误、需要拆分等四类问题
- **层级判断**: 自动判断文本块是否为新层级
- **批量处理**: 支持大量数据的批量处理

## API接口

### 处理QA问答对

**接口地址**: `POST /api/processQA`

**请求参数**:
```json
{
    "run_id": "your_run_id_here"
}
```

**响应格式**:
```json
{
    "code": "00000",
    "message": "QA问答对处理完成",
    "data": {
        "run_id": "your_run_id_here",
        "new_version": 1,
        "processed_count": 150,
        "status": "success"
    }
}
```

## 处理逻辑

### 1. 数据获取
- 从数据库读取指定run_id的版本0数据
- 按页面分组处理数据

### 2. 上下文构建
对每个文本块（除第一个和最后一个），构建包含以下内容的上下文：
- 前两个文本块
- 当前文本块（目标分析对象）
- 后一个文本块

### 3. 模型分析
使用本地训练模型对每个文本块进行分析，判断：
- **层级判断**: 是否为新层级（具有明确层级结构特征）
- **错误检测**: 是否存在四类错误
  - 字符错误：不合理字符、乱码、错误符号
  - 格式错误：句子残段、不完整开头
  - 信息错误：空文本、页码、目录等无关内容
  - 需要拆分：包含多个层级的文本块

### 4. 结果处理
- 根据AI分析结果决定是否修改文本
- 保留原始数据结构
- 生成版本1数据

## 数据格式

### 输入数据格式
```json
[
    {
        "text": "一、对月销售额10万元以下（含本数）的增值税小规模纳税人，免征增值税。",
        "page_idx": 6
    },
    {
        "text": "二、增值税小规模纳税人适用3%征收率的应税销售收入",
        "page_idx": 6
    },
    {
        "text": "，减按1%征收率征收增值税。",
        "page_idx": 7
    },
    {
        "text": "三、本公告执行至2027年12月31日。",
        "page_idx": 7
    }
]
```

### 输出数据格式
```json
{
    "text": "原始文本或处理后的文本",
    "page_index": 6,
    "text_level": 1,
    "type": "正文",
    "block_index": 2,
    "is_title_marked": false,
    "exclude_from_finetune": false,
    "remark": "因为阅读上下文第三文本块带有广义标题特征，所以判断为{新层级}。因为第三文本块因没有四类错误，所以判断为{正确}。第三文本块不做任何修改。"
}
```

## 数据库字段说明

### 新增字段：remark
- **用途**: 存储AI问答对分析结果
- **内容**: 包含层级判断和错误检测的完整分析结果
- **示例**: 
  ```
  因为阅读上下文第三文本块带有广义标题特征，所以判断为{新层级}。因为第三文本块因没有四类错误，所以判断为{正确}。第三文本块不做任何修改。
  ```

### 数据库表结构
```sql
-- 需要在pdf_json表中添加remark字段
ALTER TABLE pdf_json ADD COLUMN remark TEXT DEFAULT '';
```

## 模型配置

### 本地模型路径
- **基础模型**: `/data/training/model/Meta-Llama-3.1-8B-Instruct`
- **LoRA模型**: `/data/training/llama3.1_8b_checkpoint/20250604/checkpoint-1005`

### 模型参数
- **设备**: 自动分配GPU
- **数据类型**: bfloat16
- **最大长度**: 4096 tokens
- **生成参数**: max_new_tokens=2000, do_sample=False, num_beams=1

## 调用示例

### Python调用示例
```python
import requests
import json

def process_qa_data(run_id):
    url = "http://localhost:6667/api/processQA"
    data = {"run_id": run_id}
    
    try:
        response = requests.post(url, json=data)
        response.raise_for_status()
        result = response.json()
        
        if result.get("code") == "00000":
            print(f"处理成功: {result.get('data')}")
            return result.get("data")
        else:
            print(f"处理失败: {result.get('message')}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"请求失败: {e}")
        return None

# 使用示例
result = process_qa_data("your_run_id_here")
```

### JavaScript调用示例
```javascript
async function processQAData(runId) {
    try {
        const response = await fetch('/api/processQA', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ run_id: runId })
        });
        
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        
        const result = await response.json();
        
        if (result.code === '00000') {
            console.log('处理成功:', result.data);
            return result.data;
        } else {
            console.error('处理失败:', result.message);
            return null;
        }
    } catch (error) {
        console.error('请求失败:', error);
        return null;
    }
}

// 使用示例
processQAData('your_run_id_here');
```

## 错误处理

### 常见错误
1. **模型加载失败**: 检查模型路径和GPU可用性
2. **数据不存在**: 确认run_id对应的版本0数据存在
3. **内存不足**: 减少批处理大小或清理GPU内存
4. **处理超时**: 增加超时时间或分批处理

### 错误响应格式
```json
{
    "code": "00002",
    "message": "错误描述",
    "data": null
}
```

## 注意事项

1. **模型加载**: 首次调用需要加载模型，可能需要较长时间
2. **GPU资源**: 确保有足够的GPU内存和计算资源
3. **数据量**: 大量数据处理时建议分批进行
4. **空文本处理**: 空文本会被替换为特定标识符
5. **内存管理**: 系统会自动清理GPU内存，避免内存泄漏

## 性能优化

1. **批处理**: 支持批量处理多个文本块
2. **内存清理**: 自动清理GPU内存
3. **延迟控制**: 添加适当延迟避免GPU过载
4. **模型缓存**: 模型加载后会被缓存，提高后续调用速度

## 依赖要求

- PyTorch
- Transformers
- PEFT (Parameter-Efficient Fine-Tuning)
- CUDA支持
- 本地训练模型文件

## 启动服务

启动Flask服务：
```bash
python -m synapse_flow.web.flask_server
```

服务将在 `http://localhost:6667` 启动。 
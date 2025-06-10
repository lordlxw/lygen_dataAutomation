# Original Text 字段使用说明

## 概述

为了支持数据版本管理和对比功能，我们在 `pdf_json` 表中添加了 `original_text` 字段，用于保存版本0的原始文本内容。

## 数据库字段说明

### 新增字段：original_text
- **用途**: 保存版本0的原始文本内容
- **类型**: TEXT
- **默认值**: 空字符串
- **说明**: 用于对比和恢复，确保数据的可追溯性

### 数据库表结构
```sql
-- pdf_json表结构（新增字段）
ALTER TABLE pdf_json ADD COLUMN original_text TEXT DEFAULT '';
```

## 功能特性

### 1. 数据完整性保证
- **版本0**: 所有记录的 `original_text` 字段保存原始文本内容
- **版本1+**: 所有记录的 `original_text` 字段保存版本0的原始文本内容
- **数据对比**: 可以通过 `text` 和 `original_text` 字段对比查看数据变化

### 2. 处理逻辑
- **Text类型**: 保持原始 `text` 不变，AI分析结果存储在 `remark` 字段，原始文本保存在 `original_text` 字段
- **Table类型**: 原封不动保留，`original_text` 字段保存原始内容
- **数据顺序**: 保持原始数据的顺序不变

### 3. 数据流程
```
版本0数据 → 处理 → 版本1数据
    ↓           ↓         ↓
original_text  AI分析   original_text
    ↓           ↓         ↓
保存原始内容   remark    保存版本0内容
```

## API接口

### 1. 查询数据接口
```http
POST /api/queryPdfTextContents
Content-Type: application/json

{
    "run_id": "e46561b4-075c-47f8-80a2-efdeacb5cfa7",
    "version": 1
}
```

**返回数据格式**:
```json
{
    "code": "00000",
    "message": "查询成功",
    "records": [
        {
            "id": 1,
            "run_id": "e46561b4-075c-47f8-80a2-efdeacb5cfa7",
            "text": "当前文本内容",
            "page_index": 0,
            "text_level": 1,
            "type": "正文",
            "block_index": 0,
            "is_title_marked": false,
            "exclude_from_finetune": false,
            "remark": "AI分析结果",
            "original_text": "版本0的原始文本内容"
        }
    ]
}
```

### 2. QA处理接口
```http
POST /api/processQA
Content-Type: application/json

{
    "run_id": "e46561b4-075c-47f8-80a2-efdeacb5cfa7"
}
```

## 使用示例

### 1. 快速启动
```bash
# 启动包含original_text字段支持的服务
python quick_start_with_original_text.py
```

### 2. 测试功能
```bash
# 运行测试脚本
python test_qa_processing.py
```

### 3. 手动执行数据库迁移
```bash
# 执行SQL迁移脚本
psql -h localhost -U postgres -d synapse_flow -f add_original_text_field.sql
```

## 数据验证

### 1. 完整性检查
- 确保所有15条记录都被处理
- 验证 `original_text` 字段不为空
- 检查数据类型分布（text vs table）

### 2. 数据对比
```sql
-- 对比版本0和版本1的数据
SELECT 
    v0.text as version0_text,
    v1.text as version1_text,
    v1.original_text,
    v1.remark
FROM pdf_json v0
JOIN pdf_json v1 ON v0.run_id = v1.run_id 
    AND v0.page_index = v1.page_index 
    AND v0.block_index = v1.block_index
WHERE v0.run_id = 'e46561b4-075c-47f8-80a2-efdeacb5cfa7'
    AND v0.version = 0 
    AND v1.version = 1
ORDER BY v0.page_index, v0.block_index;
```

## 注意事项

1. **数据一致性**: 确保版本0的所有数据都有对应的 `original_text` 字段
2. **性能考虑**: `original_text` 字段会增加存储空间，但提供重要的数据追溯功能
3. **向后兼容**: 新字段不会影响现有功能，所有API保持向后兼容
4. **数据恢复**: 可以通过 `original_text` 字段恢复任何版本的原始数据

## 故障排除

### 1. 字段不存在错误
```bash
# 检查字段是否存在
python quick_start_with_original_text.py
```

### 2. 数据不完整
```sql
-- 检查数据完整性
SELECT 
    version,
    COUNT(*) as total_records,
    COUNT(original_text) as records_with_original_text,
    COUNT(remark) as records_with_remark
FROM pdf_json 
WHERE run_id = 'your_run_id'
GROUP BY version
ORDER BY version;
```

### 3. 服务启动失败
- 检查数据库连接参数
- 确认所有依赖包已安装
- 查看错误日志进行排查 
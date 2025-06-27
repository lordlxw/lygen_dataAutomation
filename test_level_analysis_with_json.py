#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
使用extracted_data.json文件测试层级分析服务
"""

import json
import os
from datetime import datetime
from synapse_flow.web.services.level_analysis_service import LevelAnalysisService

def load_test_data():
    """加载测试数据"""
    json_file = "synapse_flow/例子/output.json"
    
    if not os.path.exists(json_file):
        print(f"错误：找不到文件 {json_file}")
        return []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"成功加载测试数据，共 {len(data)} 条记录")
        return data
    except Exception as e:
        print(f"加载测试数据失败: {str(e)}")
        return []

def save_results(results, log_file_path):
    """保存分析结果到JSON文件"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f"level_analysis_results_{timestamp}.json"
    
    # 构建总的层级序列
    level_sequence = []
    for result in results:
        if result['level'] is not None:
            level_sequence.append(result['level'])
        else:
            level_sequence.append('X')  # 用X表示分析失败
    
    # 构建完整的结果数据
    complete_results = {
        "analysis_info": {
            "timestamp": datetime.now().isoformat(),
            "total_items": len(results),
            "model_info": {
                "model_name": "llama3.1_8b",
                "base_model": "Meta-Llama-3.1-8B-Instruct",
                "lora_path": "/home/liuxinwei/Models/层级训练",
                "api_endpoint": "http://localhost:8202/v1/chat/completions"
            },
            "log_file_path": log_file_path
        },
        "results": results,
        "level_sequence": level_sequence,  # 新增：总的层级序列
        "summary": {
            "successful_analysis": len([r for r in results if r['level'] is not None]),
            "failed_analysis": len([r for r in results if r['level'] is None]),
            "special_cases": len([r for r in results if r.get('is_special_case', False)]),
            "level_distribution": {}
        }
    }
    
    # 统计层级分布
    for result in results:
        if result['level'] is not None:
            level = result['level']
            complete_results["summary"]["level_distribution"][str(level)] = \
                complete_results["summary"]["level_distribution"].get(str(level), 0) + 1
    
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(complete_results, f, ensure_ascii=False, indent=2)
        print(f"分析结果已保存到: {output_file}")
        return output_file
    except Exception as e:
        print(f"保存结果失败: {str(e)}")
        return None

def print_analysis_summary(results):
    """打印分析摘要"""
    print("\n" + "="*80)
    print("层级分析摘要")
    print("="*80)
    
    successful = len([r for r in results if r['level'] is not None])
    failed = len([r for r in results if r['level'] is None])
    special_cases = len([r for r in results if r.get('is_special_case', False)])
    
    print(f"总数据条数: {len(results)}")
    print(f"成功分析: {successful}")
    print(f"分析失败: {failed}")
    print(f"特殊情况: {special_cases}")
    
    # 层级分布
    level_dist = {}
    for result in results:
        if result['level'] is not None:
            level = result['level']
            level_dist[level] = level_dist.get(level, 0) + 1
    
    print(f"\n层级分布:")
    for level in sorted(level_dist.keys()):
        print(f"  层级{level}: {level_dist[level]}条")
    
    # 总的层级序列输出
    level_sequence = []
    for result in results:
        if result['level'] is not None:
            level_sequence.append(result['level'])
        else:
            level_sequence.append('X')  # 用X表示分析失败
    
    print(f"\n总的层级序列:")
    print(f"  序列: {' → '.join(map(str, level_sequence))}")
    print(f"  长度: {len(level_sequence)}")
    print(f"  （提示：第一条自动设置为层级1，后续基于上下文判断）")
    
    # 层级结构可视化
    print(f"\n层级结构可视化:")
    current_indent = 0
    for i, (result, level) in enumerate(zip(results, level_sequence)):
        if level != 'X':
            indent = "  " * (level - 1)  # 根据层级缩进
            marker = "✓" if not result.get('is_special_case', False) else "⚠"
            special_info = f" ({result.get('special_type', '')})" if result.get('is_special_case', False) else ""
            print(f"  {indent}{marker} 层级{level}: {result['text'][:40]}{'...' if len(result['text']) > 40 else ''}{special_info}")
        else:
            print(f"  {'  ' * 2}✗ 分析失败: {result['text'][:40]}{'...' if len(result['text']) > 40 else ''}")
    
    # 特殊情况统计
    if special_cases > 0:
        special_types = {}
        for result in results:
            if result.get('is_special_case', False):
                special_type = result.get('special_type', '未知')
                special_types[special_type] = special_types.get(special_type, 0) + 1
        
        print(f"\n特殊情况类型分布:")
        for special_type, count in special_types.items():
            print(f"  {special_type}: {count}条")
    
    print("="*80)

def main():
    """主函数"""
    print("开始层级分析测试...")
    print("="*80)
    print("注意：第一条数据会自动设置为层级1，从第二条开始调用AI判断")
    print("="*80)
    
    # 1. 加载测试数据
    test_data = load_test_data()
    if not test_data:
        print("无法加载测试数据，退出程序")
        return
    
    # 2. 创建层级分析服务
    print("\n初始化层级分析服务...")
    service = LevelAnalysisService()
    print(f"日志文件路径: {service.get_log_file_path()}")
    
    # 3. 批量处理数据
    print(f"\n开始处理 {len(test_data)} 条数据...")
    results = []
    
    for i, item in enumerate(test_data):
        print(f"\n处理第 {i+1}/{len(test_data)} 条数据...")
        
        # 添加ID字段（如果没有的话）
        if 'id' not in item:
            item['id'] = f"item_{i+1:03d}"
        
        # 处理单个数据项
        result = service.process_single_item(item)
        results.append(result)
        
        # 打印简要结果
        if result['level'] is not None:
            if i == 0:  # 第一条数据
                print(f"  ✓ 层级{result['level']} (自动设置): {item['text'][:50]}...")
            else:
                print(f"  ✓ 层级{result['level']}: {item['text'][:50]}...")
            if result.get('is_special_case', False):
                print(f"    (特殊情况: {result.get('special_type', '未知')})")
        else:
            print(f"  ✗ 分析失败: {item['text'][:50]}...")
    
    # 4. 打印分析摘要
    print_analysis_summary(results)
    
    # 5. 保存结果
    output_file = save_results(results, service.get_log_file_path())
    
    # 6. 获取层级分析服务的详细信息
    hierarchy_info = service.get_level_sequence_with_contexts()
    
    # 构建总的层级序列用于显示
    total_level_sequence = []
    for result in results:
        if result['level'] is not None:
            total_level_sequence.append(result['level'])
        else:
            total_level_sequence.append('X')
    
    print(f"\n=== 层级分析结果 ===")
    print(f"总的层级序列: {' → '.join(map(str, total_level_sequence))}")
    print(f"层级序列长度: {len(total_level_sequence)}")
    print(f"层级上下文路径数量: {len(hierarchy_info['context_paths'])}")
    
    print(f"\n测试完成！")
    print(f"详细日志: {service.get_log_file_path()}")
    if output_file:
        print(f"结果文件: {output_file}")

if __name__ == "__main__":
    main() 
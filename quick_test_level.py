#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
快速测试层级分析功能
"""

import json
from synapse_flow.web.services.level_analysis_service import LevelAnalysisService

def quick_test():
    """快速测试"""
    
    # 创建服务
    service = LevelAnalysisService()
    
    # 测试数据（从extracted_data.json中选取几条）
    test_items = [
        {
            "id": "test_1",
            "text": "第一章 房地产行业概况",
            "isTitleMarked": "section level"
        },
        {
            "id": "test_2",
            "text": "第一节 业务分类",
            "isTitleMarked": "section level"
        },
        {
            "id": "test_3",
            "text": "一、依照开发主体的数量划分，房地产企业分为自行开发和合作开发：自行开发是指开发企业自己拿地、独立开发，自担开发风险、自享开发成果；合作开发是指一方提供资金，一方提供土地的开发方式，根据合作和分配方式的不同，合作开发又分为以物易物、共担风险、收取固定收益三种方式。",
            "isTitleMarked": "context level"
        },
        {
            "id": "test_4",
            "text": "第二节 业务流程",
            "isTitleMarked": "section level"
        }
    ]
    
    print("快速测试层级分析功能")
    print("="*60)
    
    results = []
    for i, item in enumerate(test_items):
        print(f"\n测试 {i+1}: {item['text'][:30]}...")
        result = service.process_single_item(item)
        results.append(result)
        
        if result['level'] is not None:
            print(f"  结果: 层级{result['level']}")
            if result.get('is_special_case', False):
                print(f"  特殊情况: {result.get('special_type', '未知')}")
        else:
            print(f"  结果: 分析失败")
    
    # 显示总的层级序列
    level_sequence = []
    for result in results:
        if result['level'] is not None:
            level_sequence.append(result['level'])
        else:
            level_sequence.append('X')
    
    print(f"\n=== 层级序列 ===")
    print(f"序列: {' → '.join(map(str, level_sequence))}")
    print(f"（提示：第一条一定是层级1，后续顺延）")
    
    # 层级结构可视化
    print(f"\n=== 层级结构 ===")
    for i, (result, level) in enumerate(zip(results, level_sequence)):
        if level != 'X':
            indent = "  " * (level - 1)
            marker = "✓" if not result.get('is_special_case', False) else "⚠"
            special_info = f" ({result.get('special_type', '')})" if result.get('is_special_case', False) else ""
            print(f"{indent}{marker} 层级{level}: {result['text'][:30]}{'...' if len(result['text']) > 30 else ''}{special_info}")
        else:
            print(f"  {'  ' * 2}✗ 分析失败: {result['text'][:30]}{'...' if len(result['text']) > 30 else ''}")
    
    print(f"\n测试完成！")
    print(f"日志文件: {service.get_log_file_path()}")
    
    # 保存简单结果
    output = {
        "test_results": results,
        "level_sequence": level_sequence,  # 新增：总的层级序列
        "level_sequence_str": " → ".join(map(str, level_sequence)),  # 新增：字符串格式
        "log_file": service.get_log_file_path()
    }
    
    with open("quick_test_results.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"结果已保存到: quick_test_results.json")

if __name__ == "__main__":
    quick_test() 
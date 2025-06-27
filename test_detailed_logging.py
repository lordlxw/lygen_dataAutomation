#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试详细的日志记录功能
"""

from synapse_flow.web.services.level_analysis_service import LevelAnalysisService

def test_detailed_logging():
    """测试详细的日志记录"""
    
    # 创建层级分析服务
    service = LevelAnalysisService()
    
    # 测试数据
    test_data = [
        {
            "id": "test_001",
            "text": "增值税纳税人与扣缴义务人有着不同的基本定义与性质，实务中应当要加以区分。",
            "isTitleMarked": "context level"
        },
        {
            "id": "test_002", 
            "text": "（二）承包、承租与挂靠",
            "isTitleMarked": "section level"
        }
    ]
    
    print("开始测试详细的日志记录功能...")
    print(f"日志文件将保存到: {service.get_log_file_path()}")
    
    # 处理测试数据
    for i, item in enumerate(test_data):
        print(f"\n{'='*100}")
        print(f"测试第 {i+1} 条数据")
        print(f"{'='*100}")
        
        result = service.process_single_item(item)
        
        print(f"\n处理结果:")
        print(f"  层级: {result['level']}")
        print(f"  推理: {result['reasoning']}")
        print(f"  特殊情况: {result['is_special_case']}")
        if result['is_special_case']:
            print(f"  特殊类型: {result['special_type']}")
    
    print(f"\n测试完成！详细日志已保存到: {service.get_log_file_path()}")

if __name__ == "__main__":
    test_detailed_logging() 
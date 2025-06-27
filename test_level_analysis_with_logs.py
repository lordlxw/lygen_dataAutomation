#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试层级分析服务的日志功能
"""

import json
from synapse_flow.web.services.level_analysis_service import LevelAnalysisService

def test_level_analysis_with_logs():
    """测试层级分析服务的日志功能"""
    
    # 测试数据
    test_data = [
        {
            "id": 1,
            "text": "企业设立管理",
            "isTitleMarked": "section level"
        },
        {
            "id": 2,
            "text": "第一节 登记信息确认",
            "isTitleMarked": "section level"
        },
        {
            "id": 3,
            "text": "三、纳税人（扣缴义务人）身份信息报告",
            "isTitleMarked": "section level"
        },
        {
            "id": 4,
            "text": "5.从事生产、经营的纳税人，应当自领取营业执照之日起30日内，持有关证件，向税务机关申报办理税务登记。",
            "isTitleMarked": "context level"
        }
    ]
    
    print("开始测试层级分析服务的日志功能...")
    
    # 初始化服务
    service = LevelAnalysisService()
    print(f"日志文件路径: {service.get_log_file_path()}")
    
    # 处理数据
    results = service.process_batch(test_data)
    
    print(f"\n处理完成！")
    print(f"共处理 {len(results)} 条数据")
    print(f"成功判断层级: {len([r for r in results if r['level'] is not None])} 条")
    print(f"详细日志已保存到: {service.get_log_file_path()}")
    
    # 显示最终结果
    print("\n=== 最终结果 ===")
    for i, result in enumerate(results):
        print(f"{i+1}. ID: {result['id']}, 文本: {result['text'][:30]}..., 层级: {result['level']}")
    
    # 显示已确认的层级
    confirmed_levels = service.get_confirmed_levels()
    print(f"\n=== 已确认的层级 ({len(confirmed_levels)} 个) ===")
    for i, level in enumerate(confirmed_levels):
        print(f"{i+1}. 层级{level['level']}: {level['text'][:30]}...")

if __name__ == "__main__":
    test_level_analysis_with_logs() 
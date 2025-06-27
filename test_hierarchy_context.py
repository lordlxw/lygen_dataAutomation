#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试层级上下文传递算法
"""

from synapse_flow.web.services.level_analysis_service import LevelAnalysisService

def test_hierarchy_context_algorithm():
    """测试层级上下文传递算法"""
    
    # 测试数据：334344444222233333
    test_levels = [3, 3, 4, 3, 4, 4, 4, 4, 4, 2, 2, 2, 2, 3, 3, 3, 3, 3]
    
    print("=== 层级上下文传递算法测试 ===")
    print(f"测试层级序列: {test_levels}")
    print()
    
    # 模拟算法执行
    current_context = []
    context_paths = []
    
    for i, level in enumerate(test_levels):
        # 记录当前元素的"上文"（处理当前元素之前的context）
        context_paths.append(current_context.copy())
        
        # 根据层级关系更新 current_context
        if not current_context:  # 如果 current_context 为空 (处理第一个元素)
            current_context.append(i)
        else:
            # 获取上一个在 current_context 中的元素的层级值
            previous_level_index_in_context = current_context[-1]
            previous_level_value = test_levels[previous_level_index_in_context]
            
            if level == previous_level_value:  # 层级相等 (同级元素)
                # 替换最后一个元素
                current_context[-1] = i
            elif level > previous_level_value:  # 层级递增 (深入子级)
                # 追加到末尾
                current_context.append(i)
            else:  # level < previous_level_value // 层级骤降 (跳出父级)
                matched_parent_index_in_context = -1
                
                # 从 current_context 末尾向前查找匹配的父级
                for j in range(len(current_context) - 1, -1, -1):
                    ancestor_index = current_context[j]
                    ancestor_level_value = test_levels[ancestor_index]
                    
                    if ancestor_level_value <= level:
                        matched_parent_index_in_context = j
                        break  # 找到第一个匹配的就停止
                
                if matched_parent_index_in_context != -1:
                    # 截断 current_context 到 matched_parent_index_in_context 的前面（不包含它）
                    # 然后将 i 追加到新的 context 列表的末尾
                    current_context = current_context[:matched_parent_index_in_context]
                    current_context.append(i)
                else:
                    # 如果没有找到匹配的父级，清空并只保留当前
                    current_context.clear()
                    current_context.append(i)
        
        print(f"Index {i} (Level {level}): Antecedent is {context_paths[i]}")
    
    print("\n=== 验证结果 ===")
    print("期望结果:")
    expected_results = [
        [], [0], [1], [1, 2], [3], [3, 4], [3, 4, 5], [3, 4, 6], [3, 4, 7], [3, 8],
        [9], [10], [11], [12], [12, 13], [12, 13, 14], [12, 13, 15], [12, 13, 16]
    ]
    
    for i, expected in enumerate(expected_results):
        actual = context_paths[i] if i < len(context_paths) else []
        status = "✅" if actual == expected else "❌"
        print(f"Index {i}: {status} 期望{expected}, 实际{actual}")
    
    # 检查是否有错误
    errors = []
    for i, expected in enumerate(expected_results):
        actual = context_paths[i] if i < len(context_paths) else []
        if actual != expected:
            errors.append(f"Index {i}: 期望{expected}, 实际{actual}")
    
    if errors:
        print(f"\n❌ 发现 {len(errors)} 个错误:")
        for error in errors:
            print(f"  {error}")
    else:
        print("\n✅ 所有测试通过！")

def test_with_real_data():
    """使用真实数据测试"""
    
    # 创建服务实例
    service = LevelAnalysisService()
    
    # 模拟真实数据
    test_data = [
        {"id": 1, "text": "第一章 总则", "isTitleMarked": "section level"},
        {"id": 2, "text": "第一条 立法目的", "isTitleMarked": "section level"},
        {"id": 3, "text": "（一）规范税收征管", "isTitleMarked": "context level"},
        {"id": 4, "text": "（二）保障财政收入", "isTitleMarked": "context level"},
        {"id": 5, "text": "1. 具体措施", "isTitleMarked": "context level"},
        {"id": 6, "text": "2. 实施步骤", "isTitleMarked": "context level"},
        {"id": 7, "text": "（1）第一步", "isTitleMarked": "context level"},
        {"id": 8, "text": "第二条 适用范围", "isTitleMarked": "section level"},
        {"id": 9, "text": "（一）适用对象", "isTitleMarked": "context level"},
        {"id": 10, "text": "（二）适用条件", "isTitleMarked": "context level"}
    ]
    
    print("\n" + "="*60)
    print("使用真实数据测试")
    print("="*60)
    
    # 模拟AI返回的层级结果
    mock_levels = [1, 2, 3, 3, 4, 4, 5, 2, 3, 3]
    
    for i, (item_data, level) in enumerate(zip(test_data, mock_levels)):
        print(f"处理: {item_data['text']} (层级{level})")
        
        # 更新层级上下文
        service.update_hierarchical_context(level, i)
        
        # 获取该位置的上文
        context_path = service.get_context_path_for_index(i)
        print(f"  上文: {context_path}")
        
        # 显示上文对应的文本
        if context_path:
            context_texts = []
            for idx in context_path:
                if idx < len(test_data):
                    context_texts.append(test_data[idx]['text'][:20] + "...")
            print(f"  上文文本: {context_texts}")
        print()
    
    # 打印最终摘要
    service.print_hierarchy_analysis_summary()

if __name__ == "__main__":
    # 运行算法测试
    test_hierarchy_context_algorithm()
    
    # 运行真实数据测试
    test_with_real_data() 
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库字段检查脚本
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_database_fields():
    """检查数据库字段状态"""
    
    # 数据库连接参数
    db_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'synapse_flow',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    try:
        conn = psycopg2.connect(**db_params)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            
            print("=== 数据库字段检查 ===")
            
            # 1. 检查pdf_json表结构
            print("\n1. 检查pdf_json表字段:")
            cur.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns 
                WHERE table_name = 'pdf_json'
                ORDER BY ordinal_position
            """)
            
            columns = cur.fetchall()
            for col in columns:
                print(f"  {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
            
            # 2. 检查特定字段是否存在
            print("\n2. 检查关键字段:")
            key_fields = ['remark', 'original_text']
            for field in key_fields:
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pdf_json' AND column_name = %s
                """, (field,))
                result = cur.fetchone()
                if result:
                    print(f"  ✅ {field} 字段存在")
                else:
                    print(f"  ❌ {field} 字段不存在")
            
            # 3. 检查数据统计
            print("\n3. 数据统计:")
            cur.execute("""
                SELECT 
                    version,
                    COUNT(*) as total_records,
                    COUNT(remark) as records_with_remark,
                    COUNT(original_text) as records_with_original_text,
                    COUNT(CASE WHEN type = 'text' OR type = '正文' THEN 1 END) as text_records,
                    COUNT(CASE WHEN type = 'table' THEN 1 END) as table_records
                FROM pdf_json 
                WHERE run_id = 'e46561b4-075c-47f8-80a2-efdeacb5cfa7'
                GROUP BY version
                ORDER BY version
            """)
            
            stats = cur.fetchall()
            for stat in stats:
                print(f"  版本 {stat['version']}:")
                print(f"    总记录数: {stat['total_records']}")
                print(f"    有remark字段: {stat['records_with_remark']}")
                print(f"    有original_text字段: {stat['records_with_original_text']}")
                print(f"    text类型: {stat['text_records']}")
                print(f"    table类型: {stat['table_records']}")
            
            # 4. 检查字段数据完整性
            print("\n4. 字段数据完整性检查:")
            cur.execute("""
                SELECT 
                    version,
                    COUNT(*) as total,
                    COUNT(CASE WHEN original_text IS NULL OR original_text = '' THEN 1 END) as empty_original_text,
                    COUNT(CASE WHEN remark IS NULL OR remark = '' THEN 1 END) as empty_remark
                FROM pdf_json 
                WHERE run_id = 'e46561b4-075c-47f8-80a2-efdeacb5cfa7'
                GROUP BY version
                ORDER BY version
            """)
            
            integrity = cur.fetchall()
            for item in integrity:
                print(f"  版本 {item['version']}:")
                print(f"    总记录: {item['total']}")
                print(f"    空original_text: {item['empty_original_text']}")
                print(f"    空remark: {item['empty_remark']}")
                
                if item['empty_original_text'] == 0:
                    print("    ✅ original_text字段完整性: 通过")
                else:
                    print(f"    ⚠️ original_text字段完整性: 有{item['empty_original_text']}条记录为空")
        
        conn.close()
        print("\n=== 检查完成 ===")
        
    except Exception as e:
        print(f"检查失败: {str(e)}")

if __name__ == "__main__":
    check_database_fields() 
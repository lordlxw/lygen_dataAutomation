#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
检查数据库层级字段脚本
"""

import psycopg2
from psycopg2.extras import RealDictCursor

def check_hierarchy_fields():
    """检查数据库层级字段"""
    
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
            
            print("=== 检查层级字段 ===")
            
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
            
            # 2. 检查层级字段是否存在
            print("\n2. 检查层级字段:")
            hierarchy_fields = ['prompt_hierarchy', 'prompt_hierarchy_reason']
            for field in hierarchy_fields:
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
            
            # 3. 如果字段不存在，提供创建SQL
            print("\n3. 创建缺失字段的SQL:")
            for field in hierarchy_fields:
                cur.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = 'pdf_json' AND column_name = %s
                """, (field,))
                result = cur.fetchone()
                if not result:
                    if field == 'prompt_hierarchy':
                        print(f"  ALTER TABLE pdf_json ADD COLUMN {field} INTEGER;")
                    else:
                        print(f"  ALTER TABLE pdf_json ADD COLUMN {field} TEXT;")
            
    except Exception as e:
        print(f"检查失败: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_hierarchy_fields() 
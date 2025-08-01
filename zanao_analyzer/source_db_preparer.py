# zanao_analyzer\source_db_preparer.py
# -*- coding: utf-8 -*-
"""
【一次性运行脚本】
用于准备原始数据库，为需要分析的表添加 'analysis_status' 字段。
这个字段将用于追踪哪些数据已经被实时处理流水线分析过。
【已根据2025-07-30的详细说明更新，可自动从config读取表名】
"""
import sqlite3
import config
import os

def add_status_column_to_table(db_path: str, table_name: str):
    """
    安全地向指定的数据库表添加 'analysis_status' 列。
    如果列已存在，则跳过。

    Args:
        db_path (str): SQLite数据库文件路径。
        table_name (str): 需要修改的表的名称。
    """
    if not os.path.exists(db_path):
        print(f"[ERROR] Database file not found at: {db_path}. Skipping.")
        return

    print(f"--- Processing database: {os.path.basename(db_path)} ---")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 检查表是否存在
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';")
        if cursor.fetchone() is None:
            print(f"[ERROR] Table '{table_name}' not found in the database. Skipping.")
            conn.close()
            return

        # 检查 'analysis_status' 列是否已存在
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'analysis_status' in columns:
            print(f"[INFO] Column 'analysis_status' already exists in table '{table_name}'. No action needed.")
        else:
            print(f"Column 'analysis_status' not found in '{table_name}'. Adding it...")
            # 添加新列，并设置默认值为 0 (表示未处理)
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN analysis_status INTEGER DEFAULT 0;")
            conn.commit()
            print(f"[SUCCESS] Successfully added 'analysis_status' column to table '{table_name}'.")

    except sqlite3.Error as e:
        print(f"[ERROR] An error occurred while processing table '{table_name}': {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
        print("-" * 50 + "\n")


if __name__ == '__main__':
    print("Starting preparation of source databases...")
    
    # 从config中动态获取所有需要处理的数据源及其配置
    for db_key, db_config in config.SOURCE_TABLES_CONFIG.items():
        db_path = config.RAW_DB_PATHS.get(db_key)
        table_name = db_config.get('table_name')

        if db_path and table_name:
            print(f"Target: '{db_key}' database, table '{table_name}'")
            add_status_column_to_table(db_path, table_name)
        else:
            print(f"[WARNING] Configuration for '{db_key}' is incomplete in config.py. Skipping.")

    print("Source database preparation process finished.")
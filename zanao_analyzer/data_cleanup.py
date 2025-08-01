# zanao_analyzer\data_cleanup.py
# -*- coding: utf-8 -*-
"""
【一键数据清理脚本】
"""
import sqlite3
import os
import config

def clear_analysis_database():
    db_path = config.ANALYSIS_DB_PATH
    if not os.path.exists(db_path):
        print(f"[INFO] Analysis database '{os.path.basename(db_path)}' not found. Nothing to clear.")
        return
    print(f"--- Clearing Analysis Database: {os.path.basename(db_path)} ---")
    tables_to_clear = ['base_analysis', 'entity_frequencies', 'user_stats', 'temporal_analysis', 'post_classifications', 'related_posts']
    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        for table in tables_to_clear:
            print(f"  Clearing table '{table}'...")
            cursor.execute(f"DELETE FROM {table};")
            cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}';")
        conn.commit()
        print("[SUCCESS] All analysis tables have been cleared.")
    except sqlite3.Error as e:
        print(f"[ERROR] An error occurred while clearing the analysis database: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()
    print("-" * 50 + "\n")

def reset_source_databases():
    print("--- Resetting Source Databases ---")
    for db_key, db_path in config.RAW_DB_PATHS.items():
        if not os.path.exists(db_path):
            print(f"[WARNING] Source database for '{db_key}' not found. Skipping.")
            continue
        table_name = config.SOURCE_TABLES_CONFIG[db_key]['table_name']
        print(f"  Resetting '{table_name}' in '{os.path.basename(db_path)}'...")
        conn = None
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name});")
            if 'analysis_status' not in [row[1] for row in cursor.fetchall()]:
                print(f"    [INFO] 'analysis_status' column not found. Skipping.")
                continue
            cursor.execute(f"UPDATE {table_name} SET analysis_status = 0;")
            conn.commit()
            print(f"    [SUCCESS] Reset {cursor.rowcount} rows in '{table_name}'.")
        except sqlite3.Error as e:
            print(f"    [ERROR] An error occurred while resetting source database '{db_key}': {e}")
            if conn: conn.rollback()
        finally:
            if conn: conn.close()
    print("-" * 50 + "\n")

if __name__ == '__main__':
    print("====== Starting Data Cleanup Process ======\n")
    user_input = input("This will ERASE all analyzed data and RESET source processing status. Are you sure? (y/n): ")
    if user_input.lower() == 'y':
        clear_analysis_database()
        reset_source_databases()
        print("====== Data Cleanup Process Finished Successfully! ======")
    else:
        print("\nCleanup process cancelled by user.")
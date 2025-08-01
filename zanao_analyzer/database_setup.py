# zanao_analyzer\database_setup.py

# 职责：【完全独立】，负责创建和初始化分析结果数据库 (analysis.db) 的所有表结构。此脚本只需在项目初始化时运行一次。

# 内容：
# 一个 setup_all_tables() 函数，包含所有 CREATE TABLE 语句，例如：
# base_analysis: 存储最基础的、每条帖子的分析结果（情感、实体JSON）。
# entity_frequencies: 存储高频实体及其统计。
# user_stats: 存储Top-K活跃用户、超级关联者/被关联者的统计数据。
# temporal_analysis: 存储逐时间段的新词、热帖走势数据。
# similarity_results: 存储帖子之间、帖子与分类体系的相似度匹配结果。

# -*- coding: utf-8 -*-
"""
【一次性运行脚本】创建和初始化分析结果数据库 - 【V2，全新表结构】
"""
import sqlite3
import config

def main():
    db_path = config.ANALYSIS_DB_PATH
    print(f"Setting up analysis database (Schema v3 - Comments Integrated) at: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # --- 核心重构：base_analysis 表 ---
        cursor.execute("DROP TABLE IF EXISTS base_analysis;") # 先删除旧表
        cursor.execute('''
        CREATE TABLE base_analysis (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_id INTEGER NOT NULL, -- 通用ID，可以是 post_id 或 comment_id
            content_type TEXT NOT NULL,  -- 'post' 或 'comment'
            user_id TEXT,
            parent_post_id INTEGER, -- 如果是评论，其所属的主贴ID
            content_created_ts INTEGER,
            analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            sentiment_label TEXT,
            sentiment_score REAL,
            entities_json TEXT,
            UNIQUE(source_db, content_type, source_id)
        );''')
        print("[SUCCESS] Recreated 'base_analysis' table with comment support.")
        
        # 表2: 实体频率
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS entity_frequencies (
            entity_text TEXT NOT NULL, entity_type TEXT NOT NULL, frequency INTEGER NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (entity_text, entity_type)
        );''')
        print("[SUCCESS] Table 'entity_frequencies' is ready.")

        # 表3: 用户统计
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id TEXT NOT NULL, stat_type TEXT NOT NULL, stat_value TEXT,
            last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (user_id, stat_type)
        );''')
        print("[SUCCESS] Table 'user_stats' is ready.")

        # 表4: 时间趋势
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS temporal_analysis (
            time_bucket TEXT NOT NULL, trend_type TEXT NOT NULL, trend_data_json TEXT,
            last_calculated TIMESTAMP DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY (time_bucket, trend_type)
        );''')
        print("[SUCCESS] Table 'temporal_analysis' is ready.")

        # --- 核心变更 ---
        cursor.execute("DROP TABLE IF EXISTS similarity_results;")
        print("[INFO] Dropped old 'similarity_results' table if it existed.")

        # 表5 (新): 帖子分类
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS post_classifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT, base_analysis_id INTEGER NOT NULL,
            source_entity_text TEXT NOT NULL, matched_classification TEXT NOT NULL,
            match_score REAL, matched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (base_analysis_id) REFERENCES base_analysis(id)
        );''')
        print("[SUCCESS] Table 'post_classifications' is ready.")
        
        # 表6 (新): 关联帖子
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS related_posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, source_post_id INTEGER NOT NULL, related_post_id INTEGER NOT NULL,
            similarity_score REAL, calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (source_post_id) REFERENCES base_analysis(id),
            FOREIGN KEY (related_post_id) REFERENCES base_analysis(id)
        );''')
        print("[SUCCESS] Table 'related_posts' is ready.")
        
        conn.commit()
        print("\nAll tables for analysis.db have been set up successfully with the new schema.")
    except sqlite3.Error as e:
        print(f"[ERROR] An error occurred while setting up the database: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == '__main__':
    main()
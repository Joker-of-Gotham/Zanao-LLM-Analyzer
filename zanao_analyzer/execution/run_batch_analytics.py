# zanao_analyzer\execution\run_batch_analytics.py

# 职责：作为一个可以定期执行（例如，每天凌晨执行一次）的脚本，负责进行消耗资源的深度分析和全局统计。
# 流程：
# 连接分析结果数据库 analysis.db。
# 调用 statistics_engine 中的各种方法，进行全局计算（Top-K用户、新词发现、热帖趋势等）。
# 将计算结果更新到 analysis.db 中对应的统计表里。
# 调用 similarity_engine，对新分析的帖子进行分类匹配，保存结果。

# -*- coding: utf-8 -*-
"""
【核心】批量分析与统计脚本 - 【V4，使用新的post_classifications表】
- 作为一个可以定期执行的脚本，负责进行消耗资源的深度分析和全局统计。
"""
import sys
import os
import sqlite3
import json
import numpy as np

# --- 动态路径修复，确保可以从任何位置运行 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
sys.path.insert(0, project_root)
# --- 路径修复结束 ---

import config
from core.statistics_engine import StatisticsEngine
from core.similarity_engine import SimilarityEngine

def run_statistics_module(conn: sqlite3.Connection):
    """运行所有统计计算模块"""
    print("\n--- Running Statistics Module (Full) ---")
    # 将 config 对象传递给 StatisticsEngine
    stats_engine = StatisticsEngine(conn, config)
    
    # 依次调用所有高级统计方法
    stats_engine.calculate_entity_frequencies()
    stats_engine.analyze_user_relations(top_k=20)
    stats_engine.track_hot_post_trends(time_window_days=7, top_k=10)
    stats_engine.detect_new_words(recent_days=7, historical_days=30, top_k=20)
    
    print("--- Statistics Module Finished ---")

def run_classification_module(conn: sqlite3.Connection):
    """
    运行帖子分类模块 (原similarity_module)，将结果存入新的 post_classifications 表。
    """
    print("\n--- Running Post Classification Module ---")
    sim_engine = SimilarityEngine() # SimilarityEngine 成功初始化
    cursor = conn.cursor()
    
    try:
        # ... (查找未分类帖子的 SQL 查询，无需修改) ...
        cursor.execute("""
            SELECT ba.id, ba.entities_json
            FROM base_analysis ba
            LEFT JOIN post_classifications pc ON ba.id = pc.base_analysis_id
            WHERE pc.id IS NULL AND ba.entities_json IS NOT NULL;
        """)
        posts_to_classify = cursor.fetchall()
        
        if not posts_to_classify:
            print("No new posts found for classification. Skipping.")
            return

        print(f"Found {len(posts_to_classify)} new posts to classify...")
        
        data_to_insert = []
        for post_id, entities_json in posts_to_classify:
            try:
                entities = json.loads(entities_json)
                entity_texts = [e['text'] for e in entities if e.get('text')]
            except (json.JSONDecodeError, TypeError):
                continue
            
            if not entity_texts: continue

            # ✅✅✅ 核心修正 ✅✅✅
            # 1. 将实体列表拼接成一个字符串
            query_text_from_entities = " ".join(entity_texts)
            
            # 2. 调用正确的、存在的方法名，并将拼接后的字符串传入
            #    我们只取最匹配的那一个分类 (top_k=1)
            matches = sim_engine.match_query_to_classification(query_text_from_entities, top_k=1)
            # 🔴 删除错误的一行: matches = sim_engine.match_entities_to_classification(entity_texts)
            
            for match in matches:
                # 准备插入新表的数据元组
                # 这里有一个小问题：我们不知道是哪个源实体匹配上了
                # 一个简单的处理是，我们记录拼接后的文本
                # 或者，我们也可以遍历所有实体，但这样效率较低
                # 先用一个简单的方式，记录第一个实体作为源实体
                source_entity = entity_texts[0] if entity_texts else 'unknown'
                
                data_to_insert.append((
                    post_id,
                    source_entity,              # 源实体
                    match['classification'],    # 匹配到的分类
                    match['score']              # 分数
                ))
        
        # 批量插入所有找到的匹配结果
        if data_to_insert:
            cursor.executemany(
                "INSERT INTO post_classifications (base_analysis_id, source_entity_text, matched_classification, match_score) VALUES (?, ?, ?, ?);",
                data_to_insert
            )
            conn.commit()
            print(f"SUCCESS: Inserted {len(data_to_insert)} new classification matches into 'post_classifications'.")
            
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed during classification module: {e}")

    print("--- Post Classification Module Finished ---")


def main():
    """主函数，按顺序执行所有批处理任务"""
    print("====== Batch Analytics Script (Schema v2) Started ======")
    conn = None
    try:
        # 使用 with 语句确保数据库连接总能被关闭
        with sqlite3.connect(config.ANALYSIS_DB_PATH) as conn:
            run_statistics_module(conn)
            run_classification_module(conn)
    except Exception as e:
        print(f"[FATAL] An unexpected error occurred: {e}")
    
    print("\n====== Batch Analytics Script Finished ======")

if __name__ == '__main__':
    main()
# zanao_analyzer\execution\run_realtime_pipeline.py

# 职责：作为一个可以持续运行的脚本，负责处理源源不断的新数据，进行基础分析。
# 流程：
# 无限循环 while True:。
# 连接原始数据库，获取一批 analysis_status=0 的新帖子。
# 对每条帖子，调用 sentiment_analyzer 和 entity_extractor。
# 将基础分析结果（情感、实体）存入 analysis.db 的 base_analysis 表。
# 回写原始数据库，更新帖子的 analysis_status 为已处理。
# 如果没有新数据，则休眠一段时间。

# -*- coding: utf-8 -*-
"""【V7 - 已全面整合评论数据】"""
import sys, os, sqlite3, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config
from core.sentiment_analyzer import SentimentAnalyzer
from core.entity_extractor import EntityExtractor

def process_data_source(db_key, content_type, sent_analyzer, ent_extractor, analysis_conn):
    print(f"--- Checking for new '{content_type}' in '{db_key}' ---")
    source_db_path = config.RAW_DB_PATHS[db_key]
    is_post = content_type == 'post'
    
    # 根据类型确定表名和列名
    if db_key == 'inschool':
        table_name = 'posts' if is_post else 'comments'
        id_col = 'thread_id' if is_post else 'comment_id'
        user_id_col = 'user_id'
        content_col = 'content'
        time_col = 'create_time_ts'
        parent_id_col = None if is_post else 'thread_id'
    else: # outschool
        table_name = 'mx_threads' if is_post else 'mx_comments'
        id_col = 'thread_id' if is_post else 'comment_id'
        user_id_col = 'user_code'
        content_col = 'content'
        time_col = 'create_time_ts'
        parent_id_col = None if is_post else 'thread_id'
        
    try:
        with sqlite3.connect(f'file:{source_db_path}?mode=ro', uri=True) as s_conn:
            s_cursor = s_conn.cursor()
            cols_to_select = [id_col, user_id_col, time_col, content_col]
            if is_post: cols_to_select.append('title')
            if parent_id_col: cols_to_select.append(parent_id_col)
            
            s_cursor.execute(f"SELECT {', '.join(cols_to_select)} FROM {table_name} WHERE analysis_status = 0 LIMIT {config.BATCH_SIZE};")
            items_to_process = s_cursor.fetchall()

        if not items_to_process: return 0
        print(f"Found {len(items_to_process)} new items. Processing...")
        
        processed_ids, data_to_insert = [], []
        a_cursor = analysis_conn.cursor()

        for item in items_to_process:
            source_id, user_id, created_ts = item[0], item[1], item[2]
            processed_ids.append(source_id)
            
            # 组合文本内容
            text = item[3] or ""
            if is_post and item[4]: text = f"{item[4]}. {text}" # title + content
            parent_id = item[5] if not is_post and len(item) > 5 else None

            if not text.strip(): continue

            sentiment = sent_analyzer.analyze(text)
            entities = ent_extractor.extract(text)
            entities_json = json.dumps(entities, ensure_ascii=False) if entities else None
            
            data_to_insert.append((
                db_key, source_id, content_type, user_id, parent_id, created_ts,
                sentiment['label'], sentiment['score'], entities_json
            ))

        if data_to_insert:
            a_cursor.executemany(
                "INSERT OR IGNORE INTO base_analysis (source_db, source_id, content_type, user_id, parent_post_id, content_created_ts, sentiment_label, sentiment_score, entities_json) VALUES (?,?,?,?,?,?,?,?,?);",
                data_to_insert
            )
            analysis_conn.commit()

        if processed_ids:
            with sqlite3.connect(source_db_path) as write_conn:
                write_conn.execute(f"UPDATE {table_name} SET analysis_status=1 WHERE {id_col} IN ({','.join(['?']*len(processed_ids))});", processed_ids)
                write_conn.commit()
        
        return len(processed_ids)
    except sqlite3.Error as e: print(f"[ERROR] DB error for {db_key}/{content_type}: {e}"); return -1

def main_loop():
    print("--- Realtime Pipeline (Comments Integrated) Started ---")
    sent_analyzer, ent_extractor = SentimentAnalyzer(), EntityExtractor()
    sources_to_process = [('inschool', 'post'), ('inschool', 'comment'), ('outschool', 'post'), ('outschool', 'comment')]
    
    while True:
        try:
            total_processed = 0
            with sqlite3.connect(config.ANALYSIS_DB_PATH) as conn:
                for db_key, content_type in sources_to_process:
                    count = process_data_source(db_key, content_type, sent_analyzer, ent_extractor, conn)
                    if count > 0: total_processed += count
            if total_processed == 0:
                print(f"No new data. Sleeping for {config.SLEEP_INTERVAL}s...")
                time.sleep(config.SLEEP_INTERVAL)
        except KeyboardInterrupt: print("\nPipeline stopped by user."); break
        except Exception as e: print(f"[FATAL] Main loop error: {e}"); time.sleep(60)

if __name__ == '__main__': main_loop()
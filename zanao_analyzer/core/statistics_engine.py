# 包含 StatisticsEngine 类，它的所有方法都读取分析结果数据库，进行二次聚合计算。
# 提供 calculate_entity_frequencies()、find_top_k_active_users()、analyze_user_relations()、track_hot_post_trends()、detect_new_words() 等方法。

# -*- coding: utf-8 -*-
"""
复杂统计分析模块 (StatisticsEngine) - 【V4，已修复merge列名和FutureWarning】
"""
import sqlite3
import json
import pandas as pd
import jieba
import numpy as np
from collections import Counter
from datetime import datetime, timedelta

class StatisticsEngine:
    """封装所有聚合统计计算的类"""

    def __init__(self, conn: sqlite3.Connection, app_config):
        self.conn = conn
        self.cursor = conn.cursor()
        self.config = app_config

    def _execute_and_fetch_df(self, query: str, db_path: str) -> pd.DataFrame:
        try:
            with sqlite3.connect(f'file:{db_path}?mode=ro', uri=True) as temp_conn:
                return pd.read_sql_query(query, temp_conn)
        except sqlite3.Error as e:
            print(f"[ERROR] Failed to query {db_path}: {e}")
            return pd.DataFrame()

    # ... calculate_entity_frequencies 和 analyze_user_relations 方法保持不变 ...
    def calculate_entity_frequencies(self):
        print("Starting: Calculate Entity Frequencies...")
        try:
            df = pd.read_sql_query("SELECT entities_json FROM base_analysis WHERE entities_json IS NOT NULL;", self.conn)
            if df.empty:
                print("No entities found to calculate frequencies."); return
            entity_counter = Counter()
            for entities_json in df['entities_json'].dropna():
                try:
                    for entity in json.loads(entities_json):
                        entity_counter[(entity.get('text'), entity.get('label'))] += 1
                except (json.JSONDecodeError, TypeError): continue
            self.cursor.execute("DELETE FROM entity_frequencies;")
            freq_data = [(text, label, count) for (text, label), count in entity_counter.items()]
            self.cursor.executemany("INSERT INTO entity_frequencies (entity_text, entity_type, frequency) VALUES (?, ?, ?);", freq_data)
            self.conn.commit()
            print(f"SUCCESS: Updated 'entity_frequencies' with {len(freq_data)} records.")
        except Exception as e:
            self.conn.rollback(); print(f"[ERROR] Failed to calculate entity frequencies: {e}")

    def analyze_user_relations(self, top_k: int = 20):
        print("Starting: Analyze User Relations...")
        all_interactions = []
        for db_key, db_path in self.config.RAW_DB_PATHS.items():
            print(f"  Processing relations in '{db_key}'...")
            s_config = self.config.SOURCE_TABLES_CONFIG[db_key]
            p_table, c_table, u_col = s_config['table_name'], 'comments' if db_key == 'inschool' else 'mx_comments', 'user_id' if db_key == 'inschool' else 'user_code'
            posts_df = self._execute_and_fetch_df(f"SELECT thread_id, {u_col} as poster_id FROM {p_table}", db_path)
            comments_df = self._execute_and_fetch_df(f"SELECT thread_id, {u_col} as commenter_id FROM {c_table}", db_path)
            if posts_df.empty or comments_df.empty: continue
            for df in [posts_df, comments_df]:
                df['thread_id'] = pd.to_numeric(df['thread_id'], errors='coerce')
                df.dropna(subset=['thread_id'], inplace=True)
            interactions_df = pd.merge(comments_df, posts_df, on='thread_id')
            interactions_df.dropna(subset=['commenter_id', 'poster_id'], inplace=True)
            all_interactions.append(interactions_df[interactions_df['commenter_id'] != interactions_df['poster_id']])
        if not all_interactions:
            print("No interaction data found."); return
        full_df = pd.concat(all_interactions, ignore_index=True)
        connectors = full_df.groupby('commenter_id')['poster_id'].nunique().nlargest(top_k).reset_index()
        connected = full_df.groupby('poster_id')['commenter_id'].nunique().nlargest(top_k).reset_index()
        try:
            self.cursor.execute("DELETE FROM user_stats WHERE stat_type IN ('super_connector', 'super_connected');")
            con_data = [(str(r['commenter_id']), 'super_connector', str(r['poster_id'])) for _, r in connectors.iterrows()]
            ced_data = [(str(r['poster_id']), 'super_connected', str(r['commenter_id'])) for _, r in connected.iterrows()]
            self.cursor.executemany("INSERT INTO user_stats (user_id, stat_type, stat_value) VALUES (?, ?, ?);", con_data + ced_data)
            self.conn.commit()
            print(f"SUCCESS: Updated user relations stats.")
        except Exception as e:
            self.conn.rollback(); print(f"[ERROR] Failed to save user relations stats: {e}")


    def track_hot_post_trends(self, time_window_days: int = 7, top_k: int = 10):
        """追踪热点帖子趋势。"""
        print("Starting: Track Hot Post Trends...")
        all_posts_data, weights = [], {'v': 0.5, 'c': 1.5, 'l': 1.0, 's': 2.0}

        for db_key, db_path in self.config.RAW_DB_PATHS.items():
            print(f"  Tracking hot posts in '{db_key}'...")
            s_config = self.config.SOURCE_TABLES_CONFIG[db_key]
            post_table, like_col, view_col, comment_col = (
                s_config['table_name'], 'like_num' if db_key == 'inschool' else 'l_count',
                'view_count', 'mark_num' if db_key == 'inschool' else 'c_count'
            )
            query = f"SELECT thread_id, {view_col}, {comment_col}, {like_col}, create_time_ts, title FROM {post_table}"
            raw_posts_df = self._execute_and_fetch_df(query, db_path)
            if not raw_posts_df.empty:
                raw_posts_df['source_db'] = db_key
                all_posts_data.append(raw_posts_df)

        if not all_posts_data:
            print("No post data for trends."); return
        
        full_posts_df = pd.concat(all_posts_data, ignore_index=True)
        
        sentiment_query = "SELECT source_db, source_id, sentiment_score FROM base_analysis WHERE content_type = 'post'"
        sentiment_df = pd.read_sql_query(sentiment_query, self.conn)
        sentiment_df.rename(columns={'source_id': 'thread_id'}, inplace=True) # 重命名以统一键名

        for df, col in [(full_posts_df, 'thread_id'), (sentiment_df, 'thread_id')]:
            df[col] = pd.to_numeric(df[col], errors='coerce'); df.dropna(subset=[col], inplace=True); df[col] = df[col].astype('int64')
        
        # --- 核心修正：使用 suffixes 参数处理同名列，并修复 FutureWarning ---
        merged_df = pd.merge(
            full_posts_df, 
            sentiment_df, 
            on=['source_db', 'thread_id'], # 使用复合键合并
            how='left',
            suffixes=('', '_sentiment') # 指定后缀，避免自动生成 _x, _y
        )
        # 使用推荐的赋值方式，而不是 inplace=True
        merged_df['sentiment_score'] = merged_df['sentiment_score'].fillna(0)
        # --- 修正结束 ---

        # 确定列名 (因为我们只合并了 sentiment_df 的一列，所以其他列名不变)
        view_col, comment_col, like_col = 'view_count', ('mark_num' if 'mark_num' in merged_df.columns else 'c_count'), ('like_num' if 'like_num' in merged_df.columns else 'l_count')

        for col in [view_col, comment_col, like_col]:
             if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)

        merged_df['hotness'] = (
            weights['v'] * np.log1p(merged_df[view_col]) + weights['c'] * merged_df[comment_col] +
            weights['l'] * merged_df[like_col] + weights['s'] * merged_df['sentiment_score']
        )
        
        cutoff_ts = int((datetime.now() - timedelta(days=time_window_days)).timestamp())
        recent_hot_posts = merged_df[merged_df['create_time_ts'] >= cutoff_ts].nlargest(top_k, 'hotness')

        hot_post_list = [{
            'source_db': row['source_db'], # <-- 使用确定的列名
            'thread_id': int(row['thread_id']), 'title': row['title'],
            'hotness_score': round(row['hotness'], 2), 'likes': int(row[like_col]), 'comments': int(row[comment_col])
        } for _, row in recent_hot_posts.iterrows()]

        try:
            today_str = datetime.now().strftime('%Y-%m-%d')
            self.cursor.execute("DELETE FROM temporal_analysis WHERE time_bucket = ? AND trend_type = 'hot_post';", (today_str,))
            if hot_post_list:
                self.cursor.execute("INSERT INTO temporal_analysis (time_bucket, trend_type, trend_data_json) VALUES (?, 'hot_post', ?);",
                                    (today_str, json.dumps(hot_post_list, ensure_ascii=False)))
            self.conn.commit()
            print(f"SUCCESS: Saved Top-{len(hot_post_list)} hot posts for today.")
        except Exception as e:
            self.conn.rollback()
            print(f"[ERROR] Failed to save hot post trends: {e}")

    # ... detect_new_words 方法保持不变 ...
    def detect_new_words(self, recent_days=7, historical_days=30, top_k=20):
        print("Starting: Detect New Words...")
        recent_cutoff = int((datetime.now() - timedelta(days=recent_days)).timestamp())
        historical_cutoff = int((datetime.now() - timedelta(days=historical_days)).timestamp())
        recent_query = f"SELECT entities_json FROM base_analysis WHERE content_created_ts >= {recent_cutoff}"
        historical_query = f"SELECT entities_json FROM base_analysis WHERE content_created_ts >= {historical_cutoff} AND content_created_ts < {recent_cutoff}"
        recent_df = pd.read_sql_query(recent_query, self.conn)
        historical_df = pd.read_sql_query(historical_query, self.conn)
        if recent_df.empty or historical_df.empty:
            print("Not enough data to detect new words."); return
        def extract_words(df):
            return [w for j in df['entities_json'].dropna() for e in json.loads(j) for w in jieba.cut(e['text']) if len(w.strip())>1]
        r_words, h_words = extract_words(recent_df), extract_words(historical_df)
        if not r_words or not h_words:
            print("Could not extract sufficient words."); return
        r_freq, h_freq = Counter(r_words), Counter(h_words)
        r_total, h_total = sum(r_freq.values()), sum(h_freq.values())
        scores = {w: (r_freq[w]/r_total) / (h_freq.get(w,0)/h_total + 1e-9) for w in r_freq}
        top_words = sorted(scores.items(), key=lambda i: i[1], reverse=True)[:top_k]
        words_json = json.dumps([{'word':w, 'score':round(s,2)} for w,s in top_words], ensure_ascii=False)
        try:
            today = datetime.now().strftime('%Y-%m-%d')
            self.cursor.execute("DELETE FROM temporal_analysis WHERE time_bucket=? AND trend_type='new_word';", (today,))
            if top_words: self.cursor.execute("INSERT INTO temporal_analysis (time_bucket, trend_type, trend_data_json) VALUES (?, 'new_word', ?);", (today, words_json))
            self.conn.commit(); print(f"SUCCESS: Saved Top-{len(top_words)} new words.")
        except Exception as e:
            self.conn.rollback(); print(f"[ERROR] Failed to save new words: {e}")
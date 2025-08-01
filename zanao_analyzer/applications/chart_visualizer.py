# zanao_analyzer\applications\chart_visualizer.py

# 职责：生成各种图表。
# 功能：提供 create_word_cloud(user_id=None)、create_sentiment_timeseries_chart()、create_sentiment_pie_chart() 等函数。
# 它们同样查询 analysis.db，并使用 matplotlib、wordcloud 等库生成图片并保存。

# -*- coding: utf-8 -*-
"""
图表与词云生成器 (ChartVisualizer) - 【V5 - 直接读取源数据生成词云】
"""
import sqlite3
import os
import json
import pandas as pd
import jieba
from datetime import datetime
from pyecharts import options as opts
from pyecharts.charts import WordCloud, Pie, Bar, Line
from collections import Counter
from typing import Optional, List
import config

class ChartVisualizer:
    # 构造函数不再需要数据库连接
    def __init__(self):
        self.output_dir = config.CHART_OUTPUT_DIR
    
    def _save_chart(self, chart, filename_prefix: str) -> str:
        """统一的图表保存方法"""
        filename = f"{filename_prefix}_{datetime.now().strftime('%Y%m%d%H%M%S')}.html"
        filepath = os.path.join(self.output_dir, filename)
        chart.render(filepath)
        print(f"Chart saved to: {filepath}")
        return filepath

    def _get_all_text_from_source(self, user_id: Optional[str] = None) -> List[str]:
        """
        【核心新方法】直接从所有原始数据库中获取帖子和评论的原文。
        """
        all_text_list = []
        
        sources = {
            'inschool': {
                'path': config.RAW_DB_PATHS['inschool'],
                'post_table': 'posts', 'comment_table': 'comments', 'user_col': 'user_id'
            },
            'outschool': {
                'path': config.RAW_DB_PATHS['outschool'],
                'post_table': 'mx_threads', 'comment_table': 'mx_comments', 'user_col': 'user_code'
            }
        }
        
        for key, source_info in sources.items():
            try:
                # 使用只读模式安全连接
                with sqlite3.connect(f"file:{source_info['path']}?mode=ro", uri=True) as conn:
                    # 查询主贴
                    post_query = f"SELECT title, content FROM {source_info['post_table']}"
                    params = []
                    if user_id:
                        post_query += f" WHERE {source_info['user_col']} = ?"
                        params.append(user_id)
                    
                    df_posts = pd.read_sql_query(post_query, conn, params=params if user_id else None)
                    all_text_list.extend(df_posts['title'].dropna())
                    all_text_list.extend(df_posts['content'].dropna())

                    # 查询评论
                    comment_query = f"SELECT content FROM {source_info['comment_table']}"
                    if user_id: comment_query += f" WHERE {source_info['user_col']} = ?"
                    
                    df_comments = pd.read_sql_query(comment_query, conn, params=params if user_id else None)
                    all_text_list.extend(df_comments['content'].dropna())

            except Exception as e:
                print(f"[ERROR] Failed to read text from source DB '{key}': {e}")
                
        return all_text_list

    def create_word_cloud_chart(self, user_id: Optional[str] = None) -> Optional[str]:
        is_global = user_id is None
        title = "全局言论词云 (主贴+评论)" if is_global else f"用户 {user_id[:10]}... 言论词云"
        print(f"Generating word cloud by reading source DB for: {'Global' if is_global else 'User ' + user_id}")

        text_list = self._get_all_text_from_source(user_id)
        if not text_list:
            print("[INFO] No text data found in source databases for this query.")
            return None
            
        full_text = " ".join(text_list)
        words = [word for word in jieba.cut(full_text) if len(word.strip()) > 1]
        if not words:
            print("[INFO] No valid words found after segmentation.")
            return None
            
        word_counter = Counter(words)
        
        word_freq_data = list(word_counter.most_common(150))
        c = (
            WordCloud()
            .add("", word_freq_data, word_size_range=[15, 100], shape='diamond')
            .set_global_opts(title_opts=opts.TitleOpts(title=title, pos_left="center"))
        )
        return self._save_chart(c, f"wordcloud_{'global' if is_global else user_id}")

    def create_sentiment_pie_chart(self) -> Optional[str]:
        """生成全局情感分布饼图 (数据源: analysis.db)"""
        print("Generating sentiment pie chart...")
        try:
            with sqlite3.connect(f"file:{config.ANALYSIS_DB_PATH}?mode=ro", uri=True) as conn:
                query = "SELECT sentiment_label, COUNT(*) as count FROM base_analysis WHERE sentiment_label IN ('positive', 'negative') GROUP BY sentiment_label"
                df = pd.read_sql_query(query, conn)
        except Exception as e:
            print(f"[ERROR] Failed to read sentiment data from analysis.db: {e}")
            return None

        if df.empty: return None
        data_pair = [list(z) for z in zip(df['sentiment_label'], df['count'])]
        c = (
            Pie().add("", data_pair, radius=["40%", "75%"])
            .set_global_opts(title_opts=opts.TitleOpts(title="全局情感分布 (主贴+评论)", pos_left="center"))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
        )
        return self._save_chart(c, "sentiment_pie")

    def create_hot_trends_chart(self) -> Optional[str]:
        """生成热点趋势条形图 (数据源: analysis.db)"""
        print("Generating hot trends chart...")
        try:
            with sqlite3.connect(f"file:{config.ANALYSIS_DB_PATH}?mode=ro", uri=True) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute("SELECT trend_data_json FROM temporal_analysis WHERE trend_type = 'hot_post' ORDER BY time_bucket DESC LIMIT 1;")
                row = cursor.fetchone()
        except Exception as e:
            print(f"[ERROR] Failed to read hot trends data from analysis.db: {e}")
            return None

        if not row or not row['trend_data_json']: return None
        
        try:
            data = json.loads(row['trend_data_json'])
            if not data: return None
            data.sort(key=lambda x: x.get('hotness_score', 0), reverse=False)
            titles = [item.get('title', 'N/A') for item in data]
            scores = [item.get('hotness_score', 0) for item in data]
        except (json.JSONDecodeError, TypeError):
            return None
            
        c = (
            Bar()
            .add_xaxis(titles)
            .add_yaxis("热度值", scores)
            .reversal_axis()
            .set_series_opts(label_opts=opts.LabelOpts(position="right"))
            .set_global_opts(
                title_opts=opts.TitleOpts(title="近期热帖Top10"),
                yaxis_opts=opts.AxisOpts(name="帖子标题", type_="category", axislabel_opts={"interval": 0, "rotate": 0}),
            )
        )
        return self._save_chart(c, "hot_trends")
    
        # --- 【新增功能】 ---
    def create_sentiment_timeseries_chart(self) -> Optional[str]:
        """生成情感随时间变化的趋势图 (数据源: analysis.db) - 最终修正版"""
        print("--- [Final Version] Generating sentiment timeseries chart... ---")
        try:
            # 使用只读模式连接数据库
            with sqlite3.connect(f"file:{config.ANALYSIS_DB_PATH}?mode=ro", uri=True) as conn:
                query = "SELECT content_created_ts, sentiment_label FROM base_analysis WHERE sentiment_label IN ('positive', 'negative') AND content_created_ts > 0"
                df = pd.read_sql_query(query, conn)
        except Exception as e:
            print(f"[ERROR] Failed to read timeseries data from analysis.db: {e}")
            return None

        if df.empty:
            print("[INFO] No timeseries data available from the database.")
            return None

        # --- 核心修正：在分组前，直接将时间戳转换为最终需要的字符串格式 ---
        # 这一步是整个解决方案的关键，它确保了后续操作的输入是稳定和正确的。
        df['date_str'] = pd.to_datetime(df['content_created_ts'], unit='s').dt.strftime('%Y-%m-%d')

        # 使用新创建的'date_str'列进行分组。
        # 这样，生成的'sentiment_counts'的索引本身就是我们需要的日期字符串。
        sentiment_counts = df.groupby(['date_str', 'sentiment_label']).size().unstack(fill_value=0)
        
        # 确保 'positive' 和 'negative' 列存在，以防某天只有一种情绪
        if 'positive' not in sentiment_counts:
            sentiment_counts['positive'] = 0
        if 'negative' not in sentiment_counts:
            sentiment_counts['negative'] = 0
        
        # (推荐) 按索引排序，确保图表X轴的时间是连续的
        sentiment_counts = sentiment_counts.sort_index()

        # --- 数据准备：直接从已是字符串的索引中获取X轴数据 ---
        dates = sentiment_counts.index.tolist()
        positive_data = sentiment_counts['positive'].tolist()
        negative_data = sentiment_counts['negative'].tolist()

        c = (
            Line()
            .add_xaxis(dates)
            .add_yaxis("正面情绪", positive_data, is_smooth=True, linestyle_opts=opts.LineStyleOpts(color="#67C23A", width=2))
            .add_yaxis("负面情绪", negative_data, is_smooth=True, linestyle_opts=opts.LineStyleOpts(color="#F56C6C", width=2))
            .set_global_opts(
                title_opts=opts.TitleOpts(title="情感数量日度趋势图", subtitle="社区整体情绪波动"),
                tooltip_opts=opts.TooltipOpts(trigger="axis"),
                xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False, axislabel_opts=opts.LabelOpts(rotate=15)), # 标签稍作倾斜，防止重叠
                yaxis_opts=opts.AxisOpts(name="发帖/评论数"),
                datazoom_opts=[opts.DataZoomOpts(), opts.DataZoomOpts(type_="inside")],
                legend_opts=opts.LegendOpts(pos_left="center")
            )
        )
        print("--- Chart generation successful. ---")
        return self._save_chart(c, "sentiment_timeseries")
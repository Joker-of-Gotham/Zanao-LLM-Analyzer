# zanao_analyzer\applications\report_generator.py

# 职责：生成文本形式的报告和预警。
# 功能：提供如 generate_negative_alert_report()、generate_top_k_user_profile()、export_resource_posts_list() 等函数。
# 这些函数直接查询 analysis.db 中的最终统计表。

# -*- coding: utf-8 -*-
"""
文本报告与预警生成器 (ReportGenerator) - 【最终版】
"""
import sqlite3
import json
from datetime import datetime

class ReportGenerator:
    def __init__(self, db_connection: sqlite3.Connection):
        self.conn = db_connection
        self.conn.row_factory = sqlite3.Row
        self.cursor = self.conn.cursor()

    def generate_user_profile(self, user_id: str) -> str:
        report_parts = [f"## 用户画像报告：`{user_id}`\n"]
        self.cursor.execute("SELECT stat_type, stat_value FROM user_stats WHERE user_id = ?;", (user_id,))
        stats = {row['stat_type']: row['stat_value'] for row in self.cursor.fetchall()}
        report_parts.append("### 关键身份标签:")
        if not stats:
            report_parts.append("- 该用户暂无特殊统计标签。")
        else:
            if 'super_connector' in stats: report_parts.append(f"- **超级连接者**: 主动与 **{stats['super_connector']}** 个不同用户互动。")
            if 'super_connected' in stats: report_parts.append(f"- **超级被连接者**: 帖子被 **{stats['super_connected']}** 个不同用户评论。")

        self.cursor.execute("SELECT sentiment_label, COUNT(*) as count FROM base_analysis WHERE user_id = ? GROUP BY sentiment_label;", (user_id,))
        sentiments = {row['sentiment_label']: row['count'] for row in self.cursor.fetchall()}
        total_posts = sum(sentiments.values())
        report_parts.append("\n### 情感倾向分析:")
        if total_posts == 0:
            report_parts.append("- 暂无该用户发表的帖子数据。")
        else:
            pos_count = sentiments.get('positive', 0)
            neg_count = sentiments.get('negative', 0)
            report_parts.append(f"- **发帖总数**: {total_posts}")
            report_parts.append(f"- **正面情绪占比**: {((pos_count/total_posts)*100):.2f}% ({pos_count}条)")
            report_parts.append(f"- **负面情绪占比**: {((neg_count/total_posts)*100):.2f}% ({neg_count}条)")
        return "\n".join(report_parts)

    def get_latest_trends(self, limit: int = 10) -> str:
        today_str = datetime.now().strftime('%Y-%m-%d')
        report_parts = [f"## {today_str} 趋势观察\n"]
        
        self.cursor.execute("SELECT trend_data_json FROM temporal_analysis WHERE trend_type = 'new_word' ORDER BY time_bucket DESC LIMIT 1;")
        new_word_row = self.cursor.fetchone()
        report_parts.append("### 🔥 新晋热词 Top 10:")
        if new_word_row and new_word_row['trend_data_json']:
            for i, item in enumerate(json.loads(new_word_row['trend_data_json'])[:limit]):
                report_parts.append(f"{i+1}. **{item['word']}** (热度分: {item['score']})")
        else:
            report_parts.append("- 今日暂无新词数据。")
            
        self.cursor.execute("SELECT trend_data_json FROM temporal_analysis WHERE trend_type = 'hot_post' ORDER BY time_bucket DESC LIMIT 1;")
        hot_post_row = self.cursor.fetchone()
        report_parts.append("\n### 🚀 近期热帖 Top 10:")
        if hot_post_row and hot_post_row['trend_data_json']:
            try:
                hot_posts = json.loads(hot_post_row['trend_data_json'])
                for i, item in enumerate(hot_posts[:limit]):
                    # --- 核心修正 ---
                    # 使用 .get() 方法安全地获取键值
                    # 将 'score' 修改为正确的 'hotness_score'
                    title = item.get('title', '无标题')
                    hotness = item.get('hotness_score', 0) # <--- 使用正确的键名
                    report_parts.append(f"{i+1}. **{title}** (热度: {hotness})")
                    # --- 修正结束 ---
            except (json.JSONDecodeError, TypeError):
                 report_parts.append("- 趋势数据格式错误。")
        else:
            report_parts.append("- 今日暂无热帖数据。")
            
        return "\n".join(report_parts)
# 全局唯一的配置来源，管理所有魔法数字和路径

# 内容：
# RAW_DB_PATH: 原始数据库的路径。
# ANALYSIS_DB_PATH: 新的分析结果数据库的路径 (D:\...\data\zanao_analyzed_info\analysis.db)。
# RESOURCE_CLASSIFICATION_FILE_PATH: 【新增】资源多级分类层级体系文件的路径（例如，一个 taxonomy.json 或 yaml 文件）。
# MODELS: 一个字典，存放所有AI模型的路径或HuggingFace名称，如 {'sentiment': '...', 'ner': '...'}。
# API_ENDPOINTS: 一个字典，存放外部API，如 {'vector_search': 'http://...'}。
# THRESHOLDS: 一个字典，存放各种阈值，如负面情绪预警阈值、相似度匹配阈值等。

# -*- coding: utf-8 -*-
"""
全局唯一的配置来源，管理所有魔法数字、路径和凭据。
【V4 - 已修正分类匹配阈值】
"""
import os

# --- 核心路径配置 ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.normpath(os.path.join(BASE_DIR, '..', 'data'))
RAW_DB_PATHS = {
    'inschool': os.path.join(DATA_DIR, r'zanao_detailed_info\inschool_posts_and_comments.db'),
    'outschool': os.path.join(DATA_DIR, r'zanao_detailed_info\outschool_mx_tags_data.db')
}
ANALYSIS_DB_DIR = os.path.join(DATA_DIR, 'zanao_analyzed_info')
ANALYSIS_DB_PATH = os.path.join(ANALYSIS_DB_DIR, 'analysis.db')

# --- 核心逻辑配置 ---
RESOURCE_CLASSIFICATION_FILE_PATH = os.path.join(BASE_DIR, 'core', 'resources', 'taxonomy.json')
NER_LABELS = ["领域", "时间", "性格", "地点", "人物", "事件", "行为", "态度"]                       # 这个模型的命名实体识别是零样本的，所以标签你可以自己定义

# --- AI模型配置 ---                           欢迎找到更加优秀的模型，并反馈给原作者
MODELS = {
    'sentiment': 'IDEA-CCNL/Erlangshen-Roberta-330M-Sentiment',
    'ner': 'knowledgator/gliner-x-large',
    'embedding': 'shibing624/text2vec-base-chinese'
}

# --- 业务逻辑阈值 ---                          这部分你也可以自己配置
THRESHOLDS = {
    'negative_sentiment_alert': 0.6, 
    'post_similarity_match': 0.85,
    # --- 核心修正：大幅降低分类匹配阈值 ---
    'entity_classification_match': 0.55, # 从 0.90 降低到 0.65
     'query_classification_match': 0.45,
    # --- 修正结束 ---
    'ner_threshold': 0.5
}

# (其余部分保持不变)
SOURCE_TABLES_CONFIG = {
    'inschool': {'table_name': 'posts', 'id_column': 'thread_id', 'content_columns': ['title', 'content']},
    'outschool': {'table_name': 'mx_threads', 'id_column': 'thread_id', 'content_columns': ['title', 'content']}
}
SENTIMENT_LABEL_MAP = {0: 'negative', 1: 'positive'}
BATCH_SIZE = 10
SLEEP_INTERVAL = 30
CHINESE_FONT_PATH = 'C:/Windows/Fonts/deng.ttf' 
CHART_OUTPUT_DIR = os.path.join(BASE_DIR, 'generated_charts')
os.makedirs(ANALYSIS_DB_DIR, exist_ok=True)
os.makedirs(os.path.join(BASE_DIR, 'core', 'resources'), exist_ok=True)
os.makedirs(CHART_OUTPUT_DIR, exist_ok=True)

API_PORT = 5060                   # API端口你也可以自己配置
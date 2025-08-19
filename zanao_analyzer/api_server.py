# api_server.py 将是一个 Flask 或 FastAPI 应用
# 它负责将 applications 和 core 层的各种功能，包装成一个个符合OpenAPI规范的API端点
# Dify平台通过调用这些API端点来使用我们的工具。

# api_server.py — FastAPI 应用，兼容 Dify 工具模式，并优化实体抽取/相似引擎只加载一次
import os
import sys
from typing import List, Optional
import sqlite3
import uvicorn
import numpy as np
import ollama
from fastapi import FastAPI, Depends, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import config
from applications.report_generator import ReportGenerator
from applications.chart_visualizer import ChartVisualizer
from core.entity_extractor import EntityExtractor
from core.similarity_engine import SimilarityEngine
from collections import defaultdict
import textwrap
from contextlib import asynccontextmanager

# 添加根路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# 单例初始化
entity_extractor: Optional[EntityExtractor] = None
similarity_engine: Optional[SimilarityEngine] = None

# 配置服务地址及端口
API_HOST = getattr(config, 'API_HOST', '0.0.0.0')
API_PORT = getattr(config, 'API_PORT', 5060)
# PUBLIC_HOST 应为裸 IP/域名，不带 http 前缀
PUBLIC_HOST = getattr(config, 'PUBLIC_HOST', '192.168.15.45')

# 生命周期事件，仅启动时执行初始化
@asynccontextmanager
async def lifespan(app: FastAPI):
    global entity_extractor, similarity_engine
    if entity_extractor is None:
        entity_extractor = EntityExtractor()
    if similarity_engine is None:
        similarity_engine = SimilarityEngine()
    yield

# 创建 FastAPI 实例
app = FastAPI(
    title="Zanao Analyzer Tools API",
    version="2.3.2",
    openapi_url="/openapi.json",
    servers=[{"url": f"http://{PUBLIC_HOST}:{API_PORT}", "description": "Dify 访问地址"}],
    lifespan=lifespan
)

# CORS 中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    allow_credentials=True,
)

# 静态文件图表
app.mount("/charts", StaticFiles(directory=config.CHART_OUTPUT_DIR), name="charts")

# 数据库依赖
def get_db():
    # 关键参数：check_same_thread=False
    # 这个参数告诉 SQLite：“请放宽线程检查，我保证会安全地使用你。”
    # 这是 FastAPI + SQLite 的标准解决方案。
    conn = sqlite3.connect(
        f"file:{config.ANALYSIS_DB_PATH}?mode=ro",
        uri=True,
        check_same_thread=False # <--- 关键在这里！
    )
    conn.row_factory = sqlite3.Row # 确保返回的行是字典形式
    try:
        yield conn
    finally:
        conn.close()

# Pydantic 模型
class ChartRequest(BaseModel):
    chart_type: str = Field(..., description="sentiment_pie|word_cloud_global|hot_trends|sentiment_timeseries")

class ChartResponse(BaseModel):
    success: bool
    chart_url: str
    message: str
    markdown_response: str

class UserProfileResponse(BaseModel):
    report_text: str
    word_cloud_url: Optional[str]

class TrendsResponse(BaseModel):
    trends_report: str

class ResourceRequest(BaseModel):
    query_text: str = Field(..., description="用户查询文本")

class ResourceResponse(BaseModel):
    message: str
    found_posts: List[str]

# 修复 "FoundPostDetail" 和 "ResourceDetailResponse" is not defined
class FoundPostDetail(BaseModel):
    """单个帖子的详细信息模型"""
    id: str
    source: str
    title: Optional[str] = None
    content: Optional[str] = None
    author: Optional[str] = None
    url: Optional[str] = None
    
class ResourceDetailResponse(BaseModel):
    """用于资源发现的最终API响应模型"""
    message: str
    found_posts: List[FoundPostDetail]

    

# 构建完整 URL
def build_url(path: str) -> str:
    return f"http://{PUBLIC_HOST}:{API_PORT}{path}"
    
# 工具：生成图表
from fastapi import Response

@app.options("/tools/generate_chart", include_in_schema=False)
@app.options("/tools/generate-chart", include_in_schema=False)
def options_generate_chart():
    return Response(status_code=204)

@app.post(
    "/tools/generate_chart",
    summary="生成分析图表",
    operation_id="generate_chart",
    response_model=ChartResponse,
    tags=["Tools"]
)
@app.post(
    "/tools/generate-chart",
    include_in_schema=False
)
def generate_chart(req: ChartRequest):
    vis = ChartVisualizer()
    gen = {
        'sentiment_pie': vis.create_sentiment_pie_chart,
        'word_cloud_global': lambda: vis.create_word_cloud_chart(user_id=None),
        'hot_trends': vis.create_hot_trends_chart,
        'sentiment_timeseries': vis.create_sentiment_timeseries_chart
    }.get(req.chart_type)
    if not gen:
        raise HTTPException(status_code=400, detail="无效的 chart_type")
    path = gen()
    if not path:
        raise HTTPException(status_code=500, detail="图表生成失败")
    url = build_url(f"/charts/{os.path.basename(path)}")
    md = f"![chart]({url})"
    return ChartResponse(success=True, chart_url=url, message="图表已生成", markdown_response=md)

# 工具：获取用户画像
@app.get(
    "/tools/user_profile",
    summary="获取用户画像",
    operation_id="user_profile",
    response_model=UserProfileResponse,
    tags=["Tools"]
)
def get_user_profile(user_id: str, db=Depends(get_db)):
    report = ReportGenerator(db).generate_user_profile(user_id)
    wc_path = ChartVisualizer().create_word_cloud_chart(user_id=user_id)
    wc_url = build_url(f"/charts/{os.path.basename(wc_path)}") if wc_path else None
    return UserProfileResponse(report_text=report, word_cloud_url=wc_url)

# 工具：发现新词与热点
@app.get(
    "/tools/discover/trends",
    summary="发现新词与热点",
    operation_id="discover_trends",
    response_model=TrendsResponse,
    tags=["Tools"]
)
def discover_trends(db=Depends(get_db)):
    trends = ReportGenerator(db).get_latest_trends()
    return TrendsResponse(trends_report=trends)

# 工具：资源发现
@app.post(
    "/tools/find/resources",
    summary="智能资源发现 (最终精确版)",
    operation_id="find_resources",
    response_model=ResourceDetailResponse,
    tags=["Tools"]
)
def find_resources(req: ResourceRequest, db=Depends(get_db)):
    query_text = req.query_text
    print("\n--- [START] Request (Final Precise Version) ---")
    print(f"[DEBUG] User query: '{query_text}'")

    # --- 步骤 1 & 2: 双重相似度匹配 (逻辑不变) ---
    initial_matches = similarity_engine.match_query_to_classification(query_text, top_k=3)
    if not initial_matches:
        return ResourceDetailResponse(message=f"抱歉，未能找到与 '{query_text}' 相关的内容。", found_posts=[])
    
    initial_classifications = [match['classification'] for match in initial_matches]
    final_db_classifications = similarity_engine.get_db_equivalent_classifications(initial_classifications, top_k=1)
    
    if not final_db_classifications:
        return ResourceDetailResponse(message=f"在分类 '{', '.join(initial_classifications)}' 下未找到帖子。", found_posts=[])
    
    print(f"[DEBUG] Final DB classifications for query: {final_db_classifications}")

    # --- 步骤 3: 使用最终分类查询 analysis.db (逻辑不变) ---
    placeholders = ','.join(['?'] * len(final_db_classifications))
    cursor = db.cursor()
    # 我们只关心帖子的ID，所以限定 content_type = 'post'
    sql_string_template = f"""
        SELECT DISTINCT ba.source_db, ba.source_id FROM post_classifications pc
        JOIN base_analysis ba ON pc.base_analysis_id=ba.id
        WHERE pc.matched_classification IN ({placeholders}) AND ba.content_type = 'post'
        ORDER BY ba.content_created_ts DESC LIMIT 20;
    """
    sql_query = textwrap.dedent(sql_string_template)
    print(f"[DEBUG] Executing SQL: {sql_query.strip()} with params: {final_db_classifications}")
    cursor.execute(sql_query, final_db_classifications)
    rows = cursor.fetchall()

    if not rows:
        return ResourceDetailResponse(message=f"在与 '{', '.join(final_db_classifications)}' 相关的分类下未找到帖子。", found_posts=[])

    # --- 步骤 4: 分组ID (逻辑不变) ---
    ids_by_source = defaultdict(list)
    for source_db, source_id in rows:
        ids_by_source[source_db].append(source_id)
    print(f"[DEBUG] Grouped IDs for detail query: {dict(ids_by_source)}")

    # --- 步骤 5: 【核心修正】连接不同DB查询详细信息 ---
    found_posts_details = []
    for source, ids in ids_by_source.items():
        db_path = config.RAW_DB_PATHS.get(source)
        if not db_path: continue
        
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as detail_conn:
                detail_conn.row_factory = sqlite3.Row
                detail_cursor = detail_conn.cursor()
                id_placeholders = ','.join(['?'] * len(ids))
                
                if source == 'inschool':
                    # 【修正】查询的 WHERE 条件从 id IN (...) 改为 thread_id IN (...)
                    # 【修正】返回的 id 也从 row['id'] 改为 row['thread_id']
                    # 【修正】返回的 author 也从 author_name 改为 nickname
                    detail_sql = f"SELECT thread_id, title, content, nickname FROM posts WHERE thread_id IN ({id_placeholders})"
                    detail_cursor.execute(detail_sql, ids)
                    for row in detail_cursor.fetchall():
                        found_posts_details.append(FoundPostDetail(
                            id=f"inschool-{row['thread_id']}",
                            source="inschool",
                            title=row['title'],
                            content=row['content'][:200] + '...' if row['content'] else None,
                            author=row['nickname']
                        ))

                elif source == 'outschool':
                    # 【修正】查询的 WHERE 条件从 id IN (...) 改为 thread_id IN (...)
                    # 【修正】返回的 id 也从 row['id'] 改为 row['thread_id']
                    # 【修正】返回的 author 也从 author 改为 nickname
                    # 从 mx_threads 表查询，列名: thread_id, title, content, nickname, school_name
                    detail_sql = f"SELECT thread_id, title, content, nickname, school_name FROM mx_threads WHERE thread_id IN ({id_placeholders})"
                    detail_cursor.execute(detail_sql, ids)
                    for row in detail_cursor.fetchall():
                        # 作者信息可以组合得更丰富
                        author_info = f"{row['nickname']} ({row['school_name']})"
                        found_posts_details.append(FoundPostDetail(
                            id=f"outschool-{row['thread_id']}",
                            source="outschool",
                            title=row['title'],
                            content=row['content'][:200] + '...' if row['content'] else None,
                            author=author_info
                        ))
        except Exception as e:
            print(f"[ERROR] Failed to query details from {db_path}: {e}")

    # 排序并返回 (逻辑不变)
    original_order_map = {f"{source}-{post_id}": i for i, (source, post_id) in enumerate(rows)}
    found_posts_details.sort(key=lambda post: original_order_map.get(post.id, 999))

    print(f"--- [END] Success: Found {len(found_posts_details)} post details. ---")
    return ResourceDetailResponse(
        message=f"为您在与 '{', '.join(final_db_classifications)}' 相关的分类下找到以下内容:",
        found_posts=found_posts_details
    )

# ===============================================================
# =================== START: ADDED CODE BLOCK ===================
# ===============================================================

# --- 额外导入 ---
import json
from datetime import datetime
from collections import Counter
from typing import Dict, Tuple
from fastapi import Query

# --- 适用于前端 UI 的 Pydantic 模型 ---
class Post(BaseModel):
    id: str
    theme: str
    title: str
    subTheme: Optional[str] = None
    username: Optional[str] = None
    score: Optional[float] = None
    content: str
    postTime: str
    viewCount: int = 0
    likeCount: int = 0
    commentCount: int = 0
    sentiment: Optional[str] = None

class HotspotPostsResponse(BaseModel):
    posts: List[Post]
    hasMore: bool

class ChartDataItem(BaseModel):
    name: str
    value: float

class SentimentTimelinePoint(BaseModel):
    date: str
    positiveRate: float
    negativeRate: float

class SentimentAnalysisData(BaseModel):
    mostPositivePosts: List[Post]
    mostNegativePosts: List[Post]
    sentimentPie: List[ChartDataItem]
    sentimentTimeline: List[SentimentTimelinePoint]

class EmergingTopic(BaseModel):
    id: str
    topicName: str
    relatedPost: Post
    emergenceTime: str
    
class ActiveUser(BaseModel):
    userId: str
    username: str
    lastActiveTime: str
    activeTheme: str
    postCount: Optional[int] = 0
    commentCount: Optional[int] = 0
    replyCount: Optional[int] = 0

# ✅ 新增一个用于包裹 ActiveUser 列表的响应模型
class ActiveUserResponse(BaseModel):
    users: List[ActiveUser]
    hasMore: bool

# ✅ 修正 UserProfileData 模型，使其与前端完全一致
class UserProfileData(BaseModel):
    userId: str
    username: str
    activityTimeline: List[Dict] # 使用 Dict 避免严格的 ActivityEvent
    topLikedPost: Optional[Post]
    topLikedComment: Optional[Dict] # 使用 Dict 避免严格的 Comment
    wordCloud: List[ChartDataItem]
    aiAnalysis: str

# --- 核心辅助函数：查询帖子详情 ---
# zanao_analyzer/api_server.py

# ✅✅✅ 核心修正：在函数定义中，补上 db: sqlite3.Connection 参数 ✅✅✅
def fetch_post_details(post_ids: List[Tuple[str, str]], db: sqlite3.Connection) -> Dict[str, Post]:
    """
    【最终正确版 v5】
    函数签名已修正，可以接收 analysis.db 的连接，以便查询主题。
    已修复 Pydantic ValidationError，对可能为 None 的数字类型做了安全处理。
    已兼容 inschool 和 outschool 两个数据源的不同表名和列名。
    """
    if not post_ids: return {}
    details_from_raw_db = {}; ids_by_source = defaultdict(list); inschool_ids_str_list = []
    for db_key, post_id in post_ids:
        ids_by_source[db_key].append(str(post_id))
        if db_key == 'inschool' or db_key == 'outschool': inschool_ids_str_list.append(str(post_id))
    for source, ids in ids_by_source.items():
        if not ids: continue
        db_path = config.RAW_DB_PATHS.get(source)
        if not db_path: continue
        try:
            with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, check_same_thread=False) as conn:
                conn.row_factory = sqlite3.Row; placeholders = ",".join(["?"] * len(ids))
                if source == 'inschool':
                    query = f"SELECT thread_id, title, content, nickname, create_time_ts, view_count, like_num FROM posts WHERE thread_id IN ({placeholders})"
                    for row in conn.execute(query, ids).fetchall():
                        post_id_str = str(row["thread_id"]); comment_count_row = conn.execute("SELECT COUNT(*) FROM comments WHERE thread_id = ?", (post_id_str,)).fetchone()
                        view_count = row["view_count"] if "view_count" in row.keys() and row["view_count"] is not None else 0
                        like_count = row["like_num"] if "like_num" in row.keys() and row["like_num"] is not None else 0
                        details_from_raw_db[f"inschool-{post_id_str}"] = { "id": f"inschool-{post_id_str}", "title": row["title"], "username": row["nickname"], "content": row["content"] or "", "postTime": datetime.fromtimestamp(row["create_time_ts"]).isoformat(), "viewCount": view_count, "likeCount": like_count, "commentCount": comment_count_row[0] if comment_count_row else 0 }
                elif source == 'outschool':
                    query = f"SELECT thread_id, title, content, nickname, create_time_ts, view_count, l_count FROM mx_threads WHERE thread_id IN ({placeholders})"
                    for row in conn.execute(query, ids).fetchall():
                        post_id_str = str(row["thread_id"]); comment_count_row = conn.execute("SELECT COUNT(*) FROM mx_comments WHERE thread_id = ?", (post_id_str,)).fetchone()
                        view_count = row["view_count"] if "view_count" in row.keys() and row["view_count"] is not None else 0
                        like_count = row["l_count"] if "l_count" in row.keys() and row["l_count"] is not None else 0
                        details_from_raw_db[f"outschool-{post_id_str}"] = { "id": f"outschool-{post_id_str}", "title": row["title"], "username": row["nickname"], "content": row["content"] or "", "postTime": datetime.fromtimestamp(row["create_time_ts"]).isoformat(), "viewCount": view_count, "likeCount": like_count, "commentCount": comment_count_row[0] if comment_count_row else 0 }
        except Exception as e:
            import traceback; print(f"[ERROR] Failed to fetch details from {db_path}: {e}"); traceback.print_exc()
    themes = {}
    theme_groups = { "兼职 / 实习": "机会类型", "校园活动": "机会类型", "奖助学金": "机会类型", "比赛/竞赛": "机会类型", "调研/志愿": "机会类型", "保研": "升学规划", "考研": "升学规划", "出国": "升学规划", "留学": "升学规划", "选课咨询": "课程相关", "学业辅导": "课程相关", "MOOC推荐": "课程相关", "校园招聘": "职业发展", "社会招聘": "职业发展", "职业技能培训": "职业发展", "简历辅导": "职业发展", "科研助理（RA）": "科研项目", "项目申报": "科研项目", "论文合作": "科研项目", "实验室招募": "科研项目", "资格证书": "考证与能力提升", "竞赛培训": "考证与能力提升", "语言考试辅导": "考证与能力提升", "资料下载": "资源共享", "书籍交换": "资源共享", "软件工具": "资源共享", "二手物品交易": "生活服务", "拼车/租房": "生活服务", "校园打车": "生活服务", "外卖团购": "生活服务", "师生问答": "社交互助", "心灵驿站": "社交互助", "兴趣社群": "社交互助", }
    if inschool_ids_str_list:
        placeholders_theme = ",".join(["?"] * len(inschool_ids_str_list)); query_theme = f"SELECT ba.source_id, pc.matched_classification FROM base_analysis AS ba JOIN post_classifications AS pc ON ba.id = pc.base_analysis_id WHERE ba.source_db IN ('inschool', 'outschool') AND ba.source_id IN ({placeholders_theme})"
        for row in db.execute(query_theme, inschool_ids_str_list).fetchall():
            post_id_str = str(row["source_id"]); sub_theme = row["matched_classification"]; group = theme_groups.get(sub_theme, "其他分类"); themes[post_id_str] = f"{group} / {sub_theme}"
    final_post_objects = {}
    for key, detail_dict in details_from_raw_db.items():
        post_id = key.split('-')[1]; detail_dict['theme'] = themes.get(post_id, "综合 / 未分类"); final_post_objects[key] = Post(**detail_dict)
    return final_post_objects

# --- API 端点 for 前端 UI ---
@app.get("/hotspot/posts", response_model=HotspotPostsResponse, tags=["Frontend UI"])
def get_hotspot_posts(
    page: int = 1, 
    limit: int = 5,
    db: sqlite3.Connection = Depends(get_db) # 这是 analysis.db 的连接
):
    """
    【优化版】
    数据来源：
    1. ID 和分数 -> analysis.db (temporal_analysis)
    2. 详细信息 -> inschool DB (posts & comments)
    3. 主题 -> analysis.db (post_classifications & base_analysis)
    """
    # --- 步骤 1: 从 analysis.db 获取【全量】热帖榜单数据 ---
    cursor = db.cursor()
    cursor.execute("SELECT trend_data_json FROM temporal_analysis WHERE trend_type = 'hot_post' ORDER BY time_bucket DESC LIMIT 1;")
    row = cursor.fetchone()
    if not row or not row["trend_data_json"]:
        return HotspotPostsResponse(posts=[], hasMore=False)
    
    hot_posts_data = json.loads(row["trend_data_json"]) # 这是一个包含所有热帖信息的列表
    all_hot_post_ids_str = [str(p["thread_id"]) for p in hot_posts_data if p["source_db"] == "inschool"]
    
    # 对全量 ID 列表进行分页，得到当前页需要展示的帖子ID
    offset = (page - 1) * limit
    paginated_ids = all_hot_post_ids_str[offset : offset + limit]
    if not paginated_ids:
        return HotspotPostsResponse(posts=[], hasMore=False)

    # --- 步骤 2: 去 inschool DB 查询这些帖子的详情 ---
    inschool_db_path = config.RAW_DB_PATHS.get("inschool")
    if not inschool_db_path:
        raise HTTPException(status_code=500, detail="Inschool DB path not configured.")

    posts_details = {}
    with sqlite3.connect(f"file:{inschool_db_path}?mode=ro", uri=True, check_same_thread=False) as inschool_conn:
        inschool_conn.row_factory = sqlite3.Row
        placeholders = ",".join(["?"] * len(paginated_ids))
        query = f"SELECT thread_id, title, content, nickname, create_time_str, view_count, like_num FROM posts WHERE thread_id IN ({placeholders})"
        for row in inschool_conn.execute(query, paginated_ids).fetchall():
            post_id = str(row["thread_id"])
            comment_count_row = inschool_conn.execute("SELECT COUNT(*) FROM comments WHERE thread_id = ?", (post_id,)).fetchone()
            posts_details[post_id] = {
                "id": f"inschool-{post_id}",
                "title": row["title"],
                "content": row["content"],
                "username": row["nickname"],
                "postTime": row["create_time_str"],
                "viewCount": row["view_count"],
                "likeCount": row["like_num"],
                "commentCount": comment_count_row[0] if comment_count_row else 0
            }

    # --- 步骤 3: 回到 analysis.db，用一张预加载的映射表来查询帖子的主题 ---
    # 为了性能，我们先一次性把所有帖子的分类信息加载到内存
    base_analysis_map = {str(row['source_id']): row['id'] for row in db.execute("SELECT id, source_id FROM base_analysis WHERE source_db = 'inschool'")}
    classification_map = {row['base_analysis_id']: row['matched_classification'] for row in db.execute("SELECT base_analysis_id, matched_classification FROM post_classifications")}
    
    theme_groups = {
      "兼职 / 实习": "机会类型", "校园活动": "机会类型", "奖助学金": "机会类型", "比赛/竞赛": "机会类型", "调研/志愿": "机会类型",
      "保研": "升学规划", "考研": "升学规划", "出国": "升学规划", "留学": "升学规划",
      "选课咨询": "课程相关", "学业辅导": "课程相关", "MOOC推荐": "课程相关",
      "校园招聘": "职业发展", "社会招聘": "职业发展", "职业技能培训": "职业发展", "简历辅导": "职业发展",
      "科研助理（RA）": "科研项目", "项目申报": "科研项目", "论文合作": "科研项目", "实验室招募": "科研项目",
      "资格证书": "考证与能力提升", "竞赛培训": "考证与能力提升", "语言考试辅导": "考证与能力提升",
      "资料下载": "资源共享", "书籍交换": "资源共享", "软件工具": "资源共享",
      "二手物品交易": "生活服务", "拼车/租房": "生活服务", "校园打车": "生活服务", "外卖团购": "生活服务",
      "师生问答": "社交互助", "心灵驿站": "社交互助", "兴趣社群": "社交互助",
    }
    
    themes = {}
    for post_id in paginated_ids:
        base_id = base_analysis_map.get(post_id)
        if base_id:
            sub_theme = classification_map.get(base_id)
            if sub_theme:
                group = theme_groups.get(sub_theme, "其他分类")
                themes[post_id] = f"{group} / {sub_theme}"

    # --- 步骤 4: 组装最终结果 ---
    result_posts = []
    hotness_map = {str(p["thread_id"]): p['hotness_score'] for p in hot_posts_data}
    
    for post_id in paginated_ids:
        if post_id in posts_details:
            detail = posts_details[post_id]
            final_post = Post(
                **detail,
                score=hotness_map.get(post_id),
                theme=themes.get(post_id, "综合 / 未分类") # 如果没有查询到主题，给一个默认值
            )
            result_posts.append(final_post)

    return HotspotPostsResponse(
        posts=result_posts,
        hasMore=len(all_hot_post_ids_str) > offset + limit
    )

@app.get("/hotspot/score-chart", response_model=List[ChartDataItem], tags=["Frontend UI"])
def get_score_chart_for_frontend(db: sqlite3.Connection = Depends(get_db)):
    """
    【优化版】
    数据源与热帖榜单统一，都来自 temporal_analysis 表。
    """
    # --- 步骤 1: 同样，从 analysis.db 获取【全量】热帖榜单数据 ---
    cursor = db.cursor()
    cursor.execute("SELECT trend_data_json FROM temporal_analysis WHERE trend_type = 'hot_post' ORDER BY time_bucket DESC LIMIT 1;")
    row = cursor.fetchone()
    if not row or not row["trend_data_json"]:
        return []

    hot_posts_data = json.loads(row["trend_data_json"])
    
    # --- 步骤 2: 取出榜单的前 5 名 ---
    # 先使用 .get('hotness_score', 0) 确保即使缺少分数也不会报错，然后进行排序
    top_5_posts = sorted(hot_posts_data, key=lambda p: p.get('hotness_score', 0), reverse=True)[:5]
    if not top_5_posts:
        return []
        
    top_5_ids = [str(p['thread_id']) for p in top_5_posts if p.get('source_db') == 'inschool']

    # --- 步骤 3: 去 inschool DB 查询这 5 篇帖子的标题 ---
    inschool_db_path = config.RAW_DB_PATHS.get("inschool")
    titles = {}
    if top_5_ids and inschool_db_path:
        with sqlite3.connect(f"file:{inschool_db_path}?mode=ro", uri=True, check_same_thread=False) as inschool_conn:
            inschool_conn.row_factory = sqlite3.Row
            placeholders = ",".join(["?"] * len(top_5_ids))
            query = f"SELECT thread_id, title FROM posts WHERE thread_id IN ({placeholders})"
            for row in inschool_conn.execute(query, top_5_ids).fetchall():
                titles[str(row['thread_id'])] = row['title']

    # --- 步骤 4: 组装成图表需要的数据格式 ---
    chart_data = []
    for post in top_5_posts:
        post_id = str(post['thread_id'])
        title = titles.get(post_id, f"帖子...{post_id[-4:]}")
        # 对标题进行截断，防止图表显示不全
        display_title = (title[:12] + '...') if len(title) > 12 else title
        chart_data.append(
            ChartDataItem(name=display_title, value=post.get('hotness_score', 0))
        )
        
    return chart_data

@app.get("/hotspot/word-cloud", response_model=List[ChartDataItem], tags=["Frontend UI"])
def get_word_cloud_data(db: sqlite3.Connection = Depends(get_db)):
    db.row_factory = sqlite3.Row
    return [ChartDataItem(name=row["entity_text"], value=row["frequency"]) for row in db.execute("SELECT entity_text, frequency FROM entity_frequencies ORDER BY frequency DESC LIMIT 100;").fetchall()]

# --- Sentiment Module ---
@app.get("/sentiment/analysis", response_model=SentimentAnalysisData, tags=["Frontend UI"])
def get_sentiment_analysis(db: sqlite3.Connection = Depends(get_db)):
    """
    【最终确认版】
    确保从数据库中请求两条记录。
    """
    # 1. 查询最积极/消极帖子的 ID
    # ✅✅✅ 关键：确保这里的 LIMIT 是 2 ✅✅✅
    positive_rows = db.execute("SELECT source_db, source_id FROM base_analysis WHERE content_type = 'post' AND sentiment_label = 'positive' ORDER BY sentiment_score DESC LIMIT 2;").fetchall()
    negative_rows = db.execute("SELECT source_db, source_id FROM base_analysis WHERE content_type = 'post' AND sentiment_label = 'negative' ORDER BY sentiment_score ASC LIMIT 2;").fetchall()
    
    pos_ids = [(r["source_db"], str(r["source_id"])) for r in positive_rows]
    neg_ids = [(r["source_db"], str(r["source_id"])) for r in negative_rows]

    # 2. 调用 fetch_post_details 获取基础信息
    positive_posts_dict = fetch_post_details(pos_ids, db)
    negative_posts_dict = fetch_post_details(neg_ids, db)
    
    # 3. 独立查询并填充 theme 字段
    all_post_ids_str = [p.id.split('-')[1] for p in list(positive_posts_dict.values()) + list(negative_posts_dict.values())]
    if all_post_ids_str:
        themes = {}
        theme_groups = { "兼职 / 实习": "机会类型", "校园活动": "机会类型", "奖助学金": "机会类型", "比赛/竞赛": "机会类型", "调研/志愿": "机会类型", "保研": "升学规划", "考研": "升学规划", "出国": "升学规划", "留学": "升学规划", "选课咨询": "课程相关", "学业辅导": "课程相关", "MOOC推荐": "课程相关", "校园招聘": "职业发展", "社会招聘": "职业发展", "职业技能培训": "职业发展", "简历辅导": "职业发展", "科研助理（RA）": "科研项目", "项目申报": "科研项目", "论文合作": "科研项目", "实验室招募": "科研项目", "资格证书": "考证与能力提升", "竞赛培训": "考证与能力提升", "语言考试辅导": "考证与能力提升", "资料下载": "资源共享", "书籍交换": "资源共享", "软件工具": "资源共享", "二手物品交易": "生活服务", "拼车/租房": "生活服务", "校园打车": "生活服务", "外卖团购": "生活服务", "师生问答": "社交互助", "心灵驿站": "社交互助", "兴趣社群": "社交互助", }
        placeholders = ",".join(["?"] * len(all_post_ids_str))
        query_theme = f"""
            SELECT ba.source_id, pc.matched_classification
            FROM base_analysis AS ba
            JOIN post_classifications AS pc ON ba.id = pc.base_analysis_id
            WHERE ba.source_db IN ('inschool', 'outschool') AND ba.source_id IN ({placeholders})
        """
        for row in db.execute(query_theme, all_post_ids_str).fetchall():
            post_id_str = str(row["source_id"])
            sub_theme = row["matched_classification"]
            group = theme_groups.get(sub_theme, "其他分类")
            themes[post_id_str] = f"{group} / {sub_theme}"
        for post in list(positive_posts_dict.values()) + list(negative_posts_dict.values()):
            post_id = post.id.split('-')[1]
            post.theme = themes.get(post_id, "综合 / 未分类")

    # 4. 查询其他图表数据 (无变化)
    pie_data = [ChartDataItem(name=r["sentiment_label"], value=r["c"]) for r in db.execute("SELECT sentiment_label, COUNT(*) as c FROM base_analysis WHERE sentiment_label IS NOT NULL GROUP BY sentiment_label;")]
    timeline_q = """
        SELECT strftime('%Y-%m-%d', content_created_ts, 'unixepoch') as day,
               SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as positive_rate,
               SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) as negative_rate
        FROM base_analysis WHERE content_created_ts >= strftime('%s', 'now', '-30 days')
        GROUP BY day ORDER BY day;
    """
    timeline_data = [SentimentTimelinePoint(date=r["day"], positiveRate=r["positive_rate"], negativeRate=r["negative_rate"]) for r in db.execute(timeline_q).fetchall()]
    
    return SentimentAnalysisData(
        mostPositivePosts=list(positive_posts_dict.values()),
        mostNegativePosts=list(negative_posts_dict.values()),
        sentimentPie=pie_data,
        sentimentTimeline=timeline_data
    )

@app.get("/sentiment/emerging-topics", response_model=List[EmergingTopic], tags=["Frontend UI"])
def get_emerging_topics(db: sqlite3.Connection = Depends(get_db)):
    db.row_factory = sqlite3.Row
    query = """
        SELECT T2.value ->> 'text' as entity_text, T1.source_db, T1.source_id, T1.content_created_ts
        FROM base_analysis AS T1, json_each(T1.entities_json) AS T2
        WHERE T1.content_created_ts > strftime('%s', 'now', '-7 days') AND T2.value ->> 'label' IN ('事件', '产品')
        GROUP BY 1 ORDER BY COUNT(1) DESC LIMIT 5;
    """
    rows = db.execute(query).fetchall()
    details = fetch_post_details([(r["source_db"], r["source_id"]) for r in rows], db)
    topics = [
        EmergingTopic(id=f"topic-{r['entity_text']}", topicName=r['entity_text'], relatedPost=details[f"{r['source_db']}-{r['source_id']}"], emergenceTime=datetime.fromtimestamp(r["content_created_ts"]).isoformat())
        for r in rows if f"{r['source_db']}-{r['source_id']}" in details
    ]
    return topics

@app.get("/sentiment/active-users", response_model=ActiveUserResponse, tags=["Frontend UI"])
def get_active_users(
    page: int = 1,
    limit: int = 3,
    type: str = Query('posts', enum=['posts', 'comments', 'replies']),
    theme: Optional[str] = None,
    db: sqlite3.Connection = Depends(get_db)
):
    # ✅ 采用 "limit + 1" 的健壮分页策略
    # 我们请求比需要的多一条数据，用来判断后面是否还有更多
    limit_plus_one = limit + 1
    offset = (page - 1) * limit
    
    if type == 'comments': order_by_col = "comment_count"
    else: order_by_col = "post_count"
        
    query = f"""
        SELECT user_id,
               COUNT(CASE WHEN content_type = 'post' THEN 1 END) as post_count,
               COUNT(CASE WHEN content_type = 'comment' THEN 1 END) as comment_count,
               MAX(content_created_ts) as last_active_ts
        FROM base_analysis
        WHERE user_id IS NOT NULL AND user_id != ''
        GROUP BY user_id ORDER BY {order_by_col} DESC
        LIMIT ? OFFSET ?;
    """
    # ✅ 使用 limit_plus_one 进行查询
    active_user_stats = db.execute(query, (limit_plus_one, offset)).fetchall()

    # ✅ 判断是否还有更多数据
    has_more = len(active_user_stats) > limit
    # ✅ 只返回前端需要的 limit 条数据
    users_to_process = active_user_stats[:limit]
    
    user_ids = [row['user_id'] for row in users_to_process]
    nicknames = {}
    if user_ids:
        inschool_db_path = config.RAW_DB_PATHS.get("inschool")
        if inschool_db_path:
            with sqlite3.connect(f"file:{inschool_db_path}?mode=ro", uri=True, check_same_thread=False) as inschool_conn:
                inschool_conn.row_factory = sqlite3.Row
                for user_id in user_ids:
                    row = inschool_conn.execute("SELECT nickname FROM posts WHERE user_id = ? ORDER BY create_time_ts DESC LIMIT 1", (user_id,)).fetchone()
                    if row and row['nickname']: nicknames[user_id] = row['nickname']
                    else:
                        row = inschool_conn.execute("SELECT nickname FROM comments WHERE user_id = ? ORDER BY create_time_ts DESC LIMIT 1", (user_id,)).fetchone()
                        if row and row['nickname']: nicknames[user_id] = row['nickname']
    
    active_users = []
    for stat in users_to_process:
        user_id = stat['user_id']
        active_users.append(ActiveUser(
            userId=user_id,
            username=nicknames.get(user_id, f"用户...{user_id[-6:]}"),
            lastActiveTime=datetime.fromtimestamp(stat['last_active_ts']).strftime('%Y-%m-%d %H:%M:%S'),
            activeTheme="综合",
            postCount=stat['post_count'],
            commentCount=stat['comment_count']
        ))
    
    # ✅ 返回新的响应模型
    return ActiveUserResponse(users=active_users, hasMore=has_more)

# --- User Module ---
@app.get("/user/profile/{user_id}", response_model=UserProfileData, tags=["Frontend UI"])
def get_user_profile_details(user_id: str, db: sqlite3.Connection = Depends(get_db)):
    # 检查用户是否存在
    user_exists_row = db.execute("SELECT 1 FROM base_analysis WHERE user_id = ? LIMIT 1", (user_id,)).fetchone()
    if not user_exists_row:
        raise HTTPException(status_code=404, detail=f"User with ID '{user_id}' not found in analysis database.")

    # 1. 生成 AI 分析 (这部分逻辑不变)
    ai_analysis = ReportGenerator(db).generate_user_profile(user_id)
    
    # 2. 计算词云 (这部分逻辑不变)
    entities_query = "SELECT entities_json FROM base_analysis WHERE user_id = ? AND entities_json IS NOT NULL;"
    all_entities = []
    for row in db.execute(entities_query, (user_id,)).fetchall():
        try:
            entities = json.loads(row["entities_json"])
            if entities: all_entities.extend([entity["text"] for entity in entities if "text" in entity])
        except (json.JSONDecodeError, TypeError): continue
    word_cloud_data = [ChartDataItem(name=text, value=count) for text, count in Counter(all_entities).most_common(50)]

    # 3. 查找最高赞帖子 (这部分逻辑不变)
    user_post_ids_tuples = [(r["source_db"], str(r["source_id"])) for r in db.execute("SELECT source_db, source_id FROM base_analysis WHERE user_id = ? AND content_type = 'post';", (user_id,)).fetchall()]
    user_posts_details = list(fetch_post_details(user_post_ids_tuples, db).values())
    top_liked_post = max(user_posts_details, key=lambda p: p.likeCount) if user_posts_details else None

    # 4. 获取用户名 (这部分逻辑不变)
    username = top_liked_post.username if top_liked_post and top_liked_post.username else f"用户...{user_id[-6:]}"

    # ✅✅✅ 5. 组装并返回【完整】的 UserProfileData 对象 ✅✅✅
    return UserProfileData(
        userId=user_id,
        username=username,
        # ✅ 确保 activityTimeline 字段存在，即使是空列表
        activityTimeline=[], 
        topLikedPost=top_liked_post,
        # ✅ 确保 topLikedComment 字段存在，即使是 None
        topLikedComment=None, 
        wordCloud=word_cloud_data,
        aiAnalysis=ai_analysis
    )

# ===============================================================
# ==================== END: ADDED CODE BLOCK ====================
# ===============================================================

# 启动服务
if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT, reload=False)

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
    conn = sqlite3.connect(config.ANALYSIS_DB_PATH)
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

# 启动服务
if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT, reload=False)

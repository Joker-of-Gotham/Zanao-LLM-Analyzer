# zanao_climber/embedding_and_compare.py
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import numpy as np
import ollama
from flask import Flask, request, jsonify, make_response
from sklearn.metrics.pairwise import cosine_similarity
import traceback

# 假设您有这两个函数来获取数据库连接
from zanao_climber.data_handler import get_posts_db_conn, get_mx_db_conn
import sqlite3  # 使用 sqlite3 作为示例

# --- OpenAPI/Swagger 相关的库 ---
from apispec import APISpec
from apispec.ext.marshmallow import MarshmallowPlugin

security_schemes = {
    "apiKeyAuth": {"type": "apiKey", "in": "header", "name": "Authorization"}
}
# 使用 MarshmallowPlugin，手动定义 path 和 operations
spec = APISpec(
    title="Zanao 数据库语义搜索服务",
    version="1.3.0",
    openapi_version="3.0.3",
    info={
        "description": "一个使用语义向量搜索 Zanao 校园论坛数据库的 API 服务",
        "components": {"securitySchemes": security_schemes}
    },
    servers=[{"url": "http://192.168.15.45:5005", "description": "本地开发服务器"}],
    plugins=[MarshmallowPlugin()],
)

app = Flask(__name__)

# CORS
@app.after_request
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type,Authorization'
    response.headers['Access-Control-Allow-Methods'] = 'GET,POST,OPTIONS'
    return response

# --- (核心修正) 数据预加载函数 ---
def load_and_vectorize_posts(db_conn, table_name, id_idx, title_idx, content_idx, time_idx):
    cursor = db_conn.cursor()

    # 1. 获取所有列的信息，并动态确定ID列的真实名称
    try:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns_info = cursor.fetchall()
        # columns 是所有列名的列表
        columns = [row[1] for row in columns_info]
        # ** 根据用户提供的id_idx，动态获取ID列的真实名称 **
        id_column_name = columns[id_idx]
        print(f"检测到主键列为: '{id_column_name}' (位于索引 {id_idx})")

        if 'embedding' not in columns:
            print(f"列 'embedding' 不存在，正在为表 '{table_name}' 添加...")
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN embedding BLOB")
            db_conn.commit()
            print("列 'embedding' 添加成功。")
            # 更新列信息
            columns.append('embedding')

    except IndexError:
        print(f"错误: 提供的 id_idx ({id_idx}) 超出了表的列数范围 (共 {len(columns)} 列)。请检查您的参数。")
        return [], None
    except Exception as e:
        print(f"错误: 检查或添加列时失败: {e}")
        return [], None

    # 2. 找出尚未被向量化的新帖子 (embedding IS NULL)
    print(f"正在从 '{table_name}' 查找需要向量化的新帖子...")
    cursor.execute(f"SELECT * FROM {table_name} WHERE embedding IS NULL")
    new_rows = cursor.fetchall()

    if new_rows:
        print(f"发现 {len(new_rows)} 条新帖子，开始进行向量化处理...")
        client = ollama.Client(host='http://127.0.0.1:11434')
        for i, row in enumerate(new_rows, 1):
            row_id = row[id_idx]
            title = str(row[title_idx] or "")
            content = str(row[content_idx] or "")
            text_to_vectorize = f"标题: {title}\n内容: {content}"

            if not text_to_vectorize.strip():
                print(f"  跳过帖子 ID {row_id}，因为内容为空。")
                continue
            
            try:
                response = client.embed(model='granite-embedding:278m', input=text_to_vectorize)
                vec = None
                if 'embeddings' in response and isinstance(response['embeddings'], list) and response['embeddings']:
                    vec = response['embeddings'][0]
                elif 'embedding' in response and isinstance(response['embedding'], list):
                    vec = response['embedding']

                if vec is not None:
                    vec_np = np.array(vec, dtype=np.float32)
                    vec_bytes = vec_np.tobytes()
                    # ** 使用动态获取的 id_column_name 来构建正确的UPDATE语句 **
                    update_query = f"UPDATE {table_name} SET embedding = ? WHERE {id_column_name} = ?"
                    cursor.execute(update_query, (vec_bytes, row_id))
                    print(f"  已处理并存储帖子 ID {row_id} 的向量 ({i}/{len(new_rows)})")
                else:
                    print(f"  无法为帖子 ID {row_id} 生成向量，跳过。")
            except Exception as e:
                print(f"  处理帖子 ID {row_id} 时出错: {e}")
                traceback.print_exc() # 打印详细错误以供调试
        db_conn.commit()
        print("新帖子向量化处理完成。")
    else:
        print("没有发现需要向量化的新帖子。")

    # 3. 从数据库加载所有已处理的帖子及其向量到内存
    print("正在从数据库加载所有已处理的帖子和向量...")
    cursor.execute(f"SELECT * FROM {table_name} WHERE embedding IS NOT NULL")
    all_valid_rows = cursor.fetchall()
    
    if not all_valid_rows:
        print("数据库中没有已向量化的数据。")
        return [], None
    
    embedding_idx = columns.index('embedding')

    successful_rows = []
    successful_embeddings = []
    for row in all_valid_rows:
        successful_rows.append(row)
        embedding_blob = row[embedding_idx]
        vec = np.frombuffer(embedding_blob, dtype=np.float32)
        successful_embeddings.append(vec)

    vectors = np.array(successful_embeddings)
    print(f"成功加载 {len(successful_rows)} 条帖子到内存，向量矩阵形状: {vectors.shape}")
    return successful_rows, vectors

# --- 预加载所有文档向量 ---
all_posts_data = []
all_posts_vectors = np.array([])
with app.app_context():
    conn_p = get_posts_db_conn()
    if conn_p:
        try:
            # 重要: 请务必确保您数据库表列的索引与下方数字匹配
            # 假设 id 在第0位, title在第3位, content在第4位, time在第2位
            rows_p, vecs_p = load_and_vectorize_posts(conn_p, 
                                                      'posts', 
                                                      id_idx=0, 
                                                      title_idx=3, 
                                                      content_idx=4, 
                                                      time_idx=2)
            if rows_p and vecs_p is not None:
                # 注意：这里的 r[3], r[4], r[2] 仍然使用硬编码的索引
                # 这部分逻辑保持您原始代码的方式
                for r in rows_p:
                    all_posts_data.append({'source': '校内帖子', 'title': r[3], 'content': r[4], 'time_str': r[2]})
                all_posts_vectors = vecs_p
        finally:
            conn_p.close()

# --- API 接口定义 ---
@app.route('/', methods=['GET'])
def health_check():
    return jsonify({'status': 'ok', 'indexed_documents': len(all_posts_data)})

@app.route('/search', methods=['POST', 'OPTIONS'])
def semantic_search():
    # 此函数及后续所有代码均保持原样，未做任何修改
    if request.method == 'OPTIONS':
        return make_response('', 204)
    if all_posts_vectors.size == 0:
        return jsonify({'search_results': []})
    try:
        data = request.get_json(force=True) or {}
        query = (data.get('query') or '').strip()
        if not query:
            return jsonify({'error': 'Query text is required'}), 400
        client = ollama.Client(host='http://127.0.0.1:11434')
        resp = client.embed(model='granite-embedding:278m', input=query)
        qv = None
        if 'embeddings' in resp and resp['embeddings']:
            qv = np.array(resp['embeddings'][0]).reshape(1, -1)
        elif 'embedding' in resp and resp['embedding']:
            qv = np.array(resp['embedding']).reshape(1, -1)
        if qv is None:
            return jsonify({'error': 'Failed to vectorize query'}), 500
        sims = cosine_similarity(qv, all_posts_vectors)[0]
        idxs = sims.argsort()[::-1][:10]
        results = []
        for i in idxs:
            if sims[i] < 0.5:
                continue
            d = all_posts_data[i]
            # 根据您上次请求，这里已包含content，保持不变
            results.append({'source': d['source'], 'time': d['time_str'], 'title': d['title'],'content': d['content'], 'score': float(sims[i])})
        return jsonify({'search_results': results})
    except Exception as e:
        traceback.print_exc()
        return jsonify({'error': f'服务器内部错误: {e}'}), 500

# --- Dify 工具封装路由 ---
@app.route('/tools/healthCheck', methods=['GET'])
def tools_health_check():
    """Tool: healthCheck"""
    return health_check()

@app.route('/tools/semanticSearch', methods=['POST'])
def tools_semantic_search():
    """Tool: semanticSearch"""
    data = request.get_json(force=True)
    with app.test_request_context(json=data):
        return semantic_search()

# --- OpenAPI 文档手动注册 (仅工具接口) ---
with app.test_request_context():
    spec.path(path='/tools/healthCheck', operations={'get': {'summary': 'Health Check (Tool)', 'operationId': 'healthCheck', 'tags': ['Tools'], 'responses': {'200': {'description': 'Tool health'}}}})
    spec.path(path='/tools/semanticSearch', operations={'post': {'summary': 'Semantic Search (Tool)', 'operationId': 'semanticSearch', 'tags': ['Tools'], 'requestBody': {'required': True, 'content': {'application/json': {'schema': {'type': 'object', 'properties': {'query': {'type': 'string', 'description': '查询文本'}}, 'required': ['query']}}}}, 'responses': {'200': {'description': 'Tool search results'}}}})

@app.route('/openapi.json')
def openapi_json():
    return jsonify(spec.to_dict())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)

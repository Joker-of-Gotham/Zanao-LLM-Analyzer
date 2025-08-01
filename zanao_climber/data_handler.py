# zanao_climber/data_handler.py

import sqlite3
import json
import time
from pathlib import Path
from datetime import datetime
from zanao_climber import config

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "zanao_detailed_info"

def _get_db_connection(db_filename: str):
    """建立并返回一个为高并发写入优化的数据库连接"""
    full_path = DATA_DIR / db_filename
    full_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(full_path), timeout=15, check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA synchronous=NORMAL;')
    return conn

# =============================================================
#  数据库 1: 普通帖子和评论
# =============================================================

def get_posts_db_conn():
    """获取普通帖子数据库的连接"""
    return _get_db_connection(config.DB_POSTS_FILENAME)

def setup_posts_db():
    """创建普通帖子数据库的表结构和视图"""
    with get_posts_db_conn() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS posts (
                thread_id TEXT PRIMARY KEY, create_time_ts INTEGER, create_time_str TEXT, 
                title TEXT, content TEXT, user_id TEXT, nickname TEXT, 
                contact_phone TEXT, contact_qq TEXT, contact_wx TEXT,
                view_count INTEGER, mark_num INTEGER, like_num INTEGER, dislike_num INTEGER
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS comments (
                comment_id TEXT PRIMARY KEY, thread_id TEXT, create_time_ts INTEGER, 
                create_time_str TEXT, content TEXT, user_id TEXT, nickname TEXT, 
                like_num INTEGER, dislike_num INTEGER,
                comment_level INTEGER,
                reply_to_nickname TEXT,
                FOREIGN KEY(thread_id) REFERENCES posts(thread_id) ON DELETE CASCADE
            )
        ''')
        conn.execute('''
            CREATE VIEW IF NOT EXISTS v_comments_with_title AS
            SELECT
                c.comment_id, c.thread_id, p.title AS post_title, c.create_time_ts,
                c.create_time_str, c.content, c.nickname, c.comment_level,
                c.reply_to_nickname, c.like_num, c.dislike_num, c.user_id
            FROM comments c JOIN posts p ON c.thread_id = p.thread_id
            ORDER BY c.thread_id, c.create_time_ts;
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_posts_create_time ON posts(create_time_ts)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_comments_thread_id ON comments(thread_id)')

def save_post_details(post_detail):
    """
    保存帖子详情 (最终确认版，使用健壮的、明确指定列名的INSERT语句)。
    """
    ts_val = post_detail.get('create_time_ts', 0)
    ts = int(ts_val) if ts_val else 0
    create_time_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None
    
    # 按照数据库表头顺序准备数据元组
    data_tuple = (
        post_detail.get('thread_id'), ts, create_time_str,
        post_detail.get('title'), post_detail.get('content'), post_detail.get('user_id'), 
        post_detail.get('nickname'), post_detail.get('contact_phone'), post_detail.get('contact_qq'), 
        post_detail.get('contact_wx'), post_detail.get('view_count'), post_detail.get('mark_num'), 
        post_detail.get('like_num'), post_detail.get('dislike_num')
    )

    # 明确列出所有要插入的14个列名，与数据库表头完全对应
    sql_query = """
        INSERT OR REPLACE INTO posts (
            thread_id, create_time_ts, create_time_str, title, content, 
            user_id, nickname, contact_phone, contact_qq, contact_wx,
            view_count, mark_num, like_num, dislike_num
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    
    try:
        with get_posts_db_conn() as conn:
            conn.execute(sql_query, data_tuple)
    except (sqlite3.Error, ValueError) as e:
        print(f"[DB Error] 保存帖子详情失败 (ID: {post_detail.get('thread_id')}): {e}")

def _flatten_comments_recursive(comments_list, level=1, parent_nickname=None):
    """递归展平评论，并记录层级和回复对象"""
    for comment in comments_list:
        reply_to = comment.get('reply_nickname', parent_nickname if level > 1 else None)
        comment_data = {**comment, 'level': level, 'reply_to': reply_to}
        yield comment_data
        if 'reply_list' in comment and comment['reply_list']:
            yield from _flatten_comments_recursive(comment['reply_list'], level + 1, comment.get('nickname'))

def save_post_comments(thread_id, comments_list):
    comments_to_save = []
    for comment_data in _flatten_comments_recursive(comments_list):
        ts_str = comment_data.get('create_time')
        ts = int(ts_str) if ts_str and ts_str.isdigit() else 0
        comments_to_save.append((
            comment_data.get('comment_id'), thread_id, ts, datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None,
            comment_data.get('content'), comment_data.get('uid'), comment_data.get('nickname'),
            comment_data.get('like_num', 0), comment_data.get('dislike_num', 0),
            comment_data.get('level'), comment_data.get('reply_to')
        ))
    if not comments_to_save: return
    try:
        with get_posts_db_conn() as conn:
            conn.executemany('INSERT OR REPLACE INTO comments (comment_id, thread_id, create_time_ts, create_time_str, content, user_id, nickname, like_num, dislike_num, comment_level, reply_to_nickname) VALUES (?,?,?,?,?,?,?,?,?,?,?)', comments_to_save)
    except sqlite3.Error as e: print(f"[DB Error] 批量保存评论失败 (Thread ID: {thread_id}): {e}")

# =============================================================
#  数据库 2: 跨校区话题 (完整、优化版)
# =============================================================

def get_mx_db_conn():
    return _get_db_connection(config.DB_MX_FILENAME)

def setup_mx_db():
    with get_mx_db_conn() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS hot_tags (
                tag_id TEXT PRIMARY KEY, name TEXT, thread_count INTEGER, 
                user_count INTEGER, view_count INTEGER, last_updated_ts INTEGER
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS mx_threads (
                thread_id TEXT PRIMARY KEY, tag_id TEXT, create_time_ts INTEGER, p_time_str TEXT, 
                title TEXT, content TEXT, user_code TEXT, nickname TEXT, school_name TEXT, 
                view_count INTEGER, c_count INTEGER, l_count INTEGER, 
                FOREIGN KEY(tag_id) REFERENCES hot_tags(tag_id) ON DELETE CASCADE
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS mx_comments (
                comment_id TEXT PRIMARY KEY, thread_id TEXT, create_time_ts INTEGER, 
                create_time_str TEXT, content TEXT, user_code TEXT, nickname TEXT, 
                like_num INTEGER, dislike_num INTEGER, comment_level INTEGER, reply_to_nickname TEXT, 
                FOREIGN KEY(thread_id) REFERENCES mx_threads(thread_id) ON DELETE CASCADE
            )
        ''')
        conn.execute('''
            CREATE VIEW IF NOT EXISTS v_mx_comments_with_details AS
            SELECT
                mc.comment_id, mc.thread_id, mt.title AS post_title, ht.name AS tag_name,
                mc.create_time_ts, mc.create_time_str, mc.content, mc.nickname,
                mc.comment_level, mc.reply_to_nickname
            FROM mx_comments mc
            JOIN mx_threads mt ON mc.thread_id = mt.thread_id
            JOIN hot_tags ht ON mt.tag_id = ht.tag_id
            ORDER BY ht.name, mt.create_time_ts, mc.create_time_ts;
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_mx_threads_tag_id ON mx_threads(tag_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_mx_comments_thread_id ON mx_comments(thread_id)')

def save_hot_tags(tags_list):
    """
    保存热门话题 (最终确认版，使用健壮的、明确指定列名的INSERT语句)。
    """
    ts = int(time.time())
    tags_to_save = [
        (tag.get('tag_id'), tag.get('name'), tag.get('thread_count'), 
         tag.get('user_count'), tag.get('view_count'), ts)
        for tag in tags_list if tag.get('tag_id')
    ]
    if not tags_to_save: return

    # 明确列出所有要插入的6个列名，与数据库表头完全对应
    sql_query = """
        INSERT OR REPLACE INTO hot_tags (
            tag_id, name, thread_count, user_count, view_count, last_updated_ts
        ) VALUES (?, ?, ?, ?, ?, ?)
    """
    try:
        with get_mx_db_conn() as conn:
            conn.executemany(sql_query, tags_to_save)
    except sqlite3.Error as e:
        print(f"[DB Error] 批量保存热门话题失败: {e}")

def save_mx_threads(tag_id, threads_list):
    threads_to_save = []
    for thread in threads_list:
        ts_str = thread.get('p_time')
        ts = int(ts_str) if ts_str and ts_str.isdigit() else 0
        p_time_str = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None
        threads_to_save.append((
            thread.get('thread_id'), tag_id, ts, p_time_str, thread.get('title'), 
            thread.get('content'), thread.get('user_code'), thread.get('nickname'), 
            thread.get('school_name'), thread.get('view_count'), 
            thread.get('c_count'), thread.get('l_count')
        ))
    if not threads_to_save: return
    try:
        with get_mx_db_conn() as conn:
            conn.executemany('''
                INSERT OR REPLACE INTO mx_threads 
                (thread_id, tag_id, create_time_ts, p_time_str, title, content, user_code, 
                nickname, school_name, view_count, c_count, l_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', threads_to_save)
    except sqlite3.Error as e:
        print(f"[DB Error] 批量保存MX帖子失败 (Tag ID: {tag_id}): {e}")

def save_mx_comments(thread_id, comments_list):
    comments_to_save = []
    for comment_data in _flatten_comments_recursive(comments_list):
        ts_str = comment_data.get('create_time')
        ts = int(ts_str) if ts_str and ts_str.isdigit() else 0
        comments_to_save.append((
            comment_data.get('comment_id'), thread_id, ts, datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S') if ts else None,
            comment_data.get('content'),
            comment_data.get('user_code'), # MX评论中使用 user_code
            comment_data.get('nickname'), 
            comment_data.get('like_num', 0), 
            comment_data.get('dislike_num', 0), 
            comment_data.get('level'), 
            comment_data.get('reply_to')
        ))
    if not comments_to_save: return
    try:
        with get_mx_db_conn() as conn:
            conn.executemany('''
                INSERT OR REPLACE INTO mx_comments 
                (comment_id, thread_id, create_time_ts, create_time_str, content, user_code, 
                nickname, like_num, dislike_num, comment_level, reply_to_nickname)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', comments_to_save)
    except sqlite3.Error as e:
        print(f"[DB Error] 批量保存MX评论失败 (Thread ID: {thread_id}): {e}")

# =============================================================
#  总初始化函数
# =============================================================
def setup_all_databases():
    """初始化所有数据库和表结构。"""
    print("[DB] 正在初始化所有数据库...")
    print(f"[DB_DEBUG] 所有数据库文件将被创建于: {DATA_DIR}")
    setup_posts_db()
    setup_mx_db()
    print("[DB] 数据库初始化完成。")
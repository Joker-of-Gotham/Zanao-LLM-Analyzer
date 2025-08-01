# zanao_climber/crawler.py

import requests
import time
from zanao_climber import config, utils
import warnings
from urllib3.exceptions import InsecureRequestWarning

warnings.simplefilter('ignore', InsecureRequestWarning)

def _make_request(url, data, user_token, school_alias, max_retries=3):
    """一个包含重试和智能退避逻辑的内部请求函数"""
    headers = utils.get_headers(user_token, school_alias)
    for attempt in range(max_retries):
        try:
            response = requests.post(url, headers=headers, data=data, verify=False, timeout=15)
            if response.status_code == 429:
                print(f"[网络错误] 服务器返回429，请求过于频繁。将进行长时退避...")
                time.sleep(60 * (attempt + 1))
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            backoff_time = 5 * (attempt + 1)
            print(f"[网络警告] 第 {attempt + 1}/{max_retries} 次请求失败: {e}。将在 {backoff_time} 秒后重试...")
            time.sleep(backoff_time)
    print(f"[网络错误] 已达到最大重试次数，放弃请求 {url}")
    return None

# --- 爬取链 A: 普通帖子 ---
def fetch_post_list(user_token: str, school_alias: str, from_time=0):
    """1. 获取普通帖子列表"""
    url = config.THREAD_LIST_URL
    data = {'from_time': from_time, 'with_comment': 'true', 'with_reply': 'true'}
    try:
        json_data = _make_request(url, data, user_token, school_alias)
        if json_data and json_data.get('errno') == 0:
            data_part = json_data.get('data', {})
            posts_raw = data_part.get('list', [])
            earliest_time = min((int(p['p_time']) for p in posts_raw if p.get('p_time')), default=None)
            ids_and_times = [(p['thread_id'], int(p['p_time'])) for p in posts_raw if p.get('thread_id') and p.get('p_time')]
            return ids_and_times, earliest_time
        return [], None
    except Exception as e:
        print(f"[逻辑错误] 处理帖子列表响应时出错: {e}")
        return [], None

def fetch_post_details(post_id: str, user_token: str, school_alias: str):
    """1.1. 获取单个普通帖子的详情 (返回包含t_sign的完整data部分)"""
    url = config.THREAD_INFO_URL
    data = {'id': post_id}
    try:
        json_data = _make_request(url, data, user_token, school_alias)
        if json_data and json_data.get('errno') == 0:
            return json_data.get('data')
        return None
    except Exception as e:
        print(f"[逻辑错误] 处理帖子详情响应时出错 (ID: {post_id}): {e}")
        return None

def fetch_post_comments(post_id: str, t_sign: str, user_token: str, school_alias: str, from_id=0):
    """2. 获取单个普通帖子的评论列表 (需要t_sign)"""
    url = config.COMMENT_LIST_URL
    # --- 已修改：修正参数名为 'id'，并添加 'with_hongbao' ---
    data = {'id': post_id, 'sign': t_sign, 'from_id': from_id, 'with_hongbao': 0}
    try:
        json_data = _make_request(url, data, user_token, school_alias)
        if json_data and json_data.get('errno') == 0:
            return json_data.get('data')
        return None
    except Exception as e:
        print(f"[逻辑错误] 处理评论列表响应时出错 (ID: {post_id}): {e}")
        return None

# --- 爬取链 B: 跨校区话题 ---
def fetch_hot_tags(user_token: str, school_alias: str):
    url = config.MX_TAG_HOT_URL
    json_data = _make_request(url, {}, user_token, school_alias)
    if json_data and json_data.get('errno') == 0: return json_data.get('data')
    return None

def fetch_tag_threadlist(tag_id: str, user_token: str, school_alias: str, from_time=0):
    url = config.MX_TAG_THREADLIST_URL
    data = {'tag_id': tag_id, 'from_time': from_time}
    json_data = _make_request(url, data, user_token, school_alias)
    if json_data and json_data.get('errno') == 0:
        posts = json_data.get('data', {}).get('list', [])
        earliest = min((int(p['p_time']) for p in posts if p.get('p_time')), default=None)
        ids_times = [(p['thread_id'], int(p['p_time'])) for p in posts if p.get('thread_id')]
        return ids_times, earliest
    return [], None

def fetch_mx_thread_info(thread_id: str, user_token: str, school_alias: str):
    """5. 获取单个话题下的帖子详情"""
    url = config.MX_THREAD_INFO_URL
    # --- 核心修复：将参数名从 'thread_id' 改为 'id' ---
    data = {'id': thread_id}
    try:
        json_data = _make_request(url, data, user_token, school_alias)
        if json_data and json_data.get('errno') == 0:
            return json_data.get('data')
        return None
    except Exception as e:
        print(f"[逻辑错误] 处理MX帖子详情响应时出错 (ID: {thread_id}): {e}")
        return None

def fetch_mx_comment_list(thread_id: str, t_sign: str, user_token: str, school_alias: str, from_id=0):
    """6. 获取单个话题下帖子的评论列表"""
    url = config.MX_COMMENT_LIST_URL
    # --- 确认：根据您的另一个Fiddler样本，这里的 Body 是 id=...&sign=... ---
    # 所以，我们这里的参数名也应该是 'id'，而不是 'thread_id'
    data = {'id': thread_id, 'sign': t_sign, 'from_id': from_id}
    try:
        json_data = _make_request(url, data, user_token, school_alias)
        if json_data and json_data.get('errno') == 0:
            return json_data.get('data')
        return None
    except Exception as e:
        print(f"[逻辑错误] 处理MX评论列表响应时出错 (ID: {thread_id}): {e}")
        return None
# zanao_climber/worker.py

import redis, json, time, random, threading
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from zanao_climber import config, crawler, data_handler

# --- 全局变量，用于进度条和线程控制 ---
processed_count = 0
pbar = None
stop_event = threading.Event()

def dispatch_task(r, task_type, payload):
    task = {'type': task_type, 'payload': payload}
    r.rpush(config.REDIS_QUEUE_NAME, json.dumps(task))

def _retry_task(r, task, e):
    """将失败的任务重新放回队列尾部，并记录重试次数"""
    retries = task.get('retries', 0)
    # 尝试从两种可能的payload结构中获取ID
    payload = task.get('payload', {})
    post_id = payload.get('post_id') or payload.get('thread_id')
    
    if retries < config.MAX_TASK_RETRIES:
        task['retries'] = retries + 1
        tqdm.write(f"[Worker] [重试] 任务 {post_id} 失败 ({e})，将在稍后重试 ({task['retries']}/{config.MAX_TASK_RETRIES})...")
        # 使用rpush放到队尾，避免立即重试
        r.rpush(config.REDIS_QUEUE_NAME, json.dumps(task))
    else:
        tqdm.write(f"[Worker] [失败] 任务 {post_id} 已达到最大重试次数，放弃。")

def _fetch_all_comments(fetch_func, thread_id, *args):
    """通用的评论翻页获取函数"""
    all_comments, next_from_id, page = [], '0', 0
    while not stop_event.is_set():
        page += 1
        response = fetch_func(thread_id, *args, from_id=next_from_id)
        if not isinstance(response, dict) or not response.get('list'):
            if page == 1: tqdm.write(f"  > 帖子 {thread_id} 暂无评论或获取失败。")
            break
        page_comments = response['list']
        all_comments.extend(page_comments)
        if not response.get('has_more', False): break
        last_id = response.get('last_id') or response.get('next_from_id')
        if last_id and str(last_id) != '0' and str(last_id) != next_from_id:
            next_from_id = str(last_id)
            time.sleep(config.WORKER_BASE_DELAY + random.uniform(0, config.WORKER_RANDOM_DELAY))
        else: break
    if page > 1 and all_comments: tqdm.write(f"  > 帖子 {thread_id} 的评论已全部获取，共 {len(all_comments)} 条。")
    return all_comments

# =============================================================
#  爬取链 A: 普通帖子 -> 详情 -> 评论
# =============================================================
def process_chain_a(r, payload, task):
    post_id, post_time = payload.get('post_id'), payload.get('post_time')
    user_token, school_alias = payload.get('user_token'), payload.get('school_alias')
    if not all([post_id, post_time, user_token, school_alias]): return

    details_response = crawler.fetch_post_details(post_id, user_token, school_alias)
    if not (details_response and 'detail' in details_response):
        _retry_task(r, task, "详情为空或获取失败")
        return
    
    post_detail = details_response['detail']
    post_detail['create_time_ts'] = post_time
    data_handler.save_post_details(post_detail)
    
    t_sign = details_response.get('t_sign')
    if not t_sign:
        tqdm.write(f"[Worker] [警告] 帖子 {post_id} 无't_sign'，无法获取评论。")
        return
        
    all_comments = _fetch_all_comments(crawler.fetch_post_comments, post_id, t_sign, user_token, school_alias)
    if all_comments:
        data_handler.save_post_comments(post_id, all_comments)

# =================================================================
#  爬取链 B: 热门话题 -> 话题内帖子 -> (详情 -> 评论)
# =================================================================
def process_chain_b_start(r, payload, task_dict):
    """处理链B的起始任务，现在接收 task_dict 用于重试"""
    user_token, school_alias = payload['user_token'], payload['school_alias']
    hot_tags_response = crawler.fetch_hot_tags(user_token, school_alias)
    if not (hot_tags_response and 'list' in hot_tags_response):
        _retry_task(r, task_dict, "获取热门话题失败", max_retries=config.MAX_LIST_RETRIES)
        return

    hot_tags_list = hot_tags_response['list']
    data_handler.save_hot_tags(hot_tags_list)
    for tag in hot_tags_list:
        if tag.get('tag_id'):
            dispatch_task(r, 'process_chain_b_get_threads', {'tag_id': tag['tag_id'], 'user_token': user_token, 'school_alias': school_alias})
    tqdm.write(f"[Worker] 已为 {len(hot_tags_list)} 个热门话题分发子任务。")

def process_chain_b_get_threads(r, payload, task_dict):
    """处理链B的第二步，现在接收 task_dict 用于重试"""
    tag_id, user_token, school_alias = payload['tag_id'], payload['user_token'], payload['school_alias']
    
    # 翻页逻辑应该由生产者完成，Worker只处理单个任务
    # 为了简化和健壮性，我们让Worker每次只处理一页
    thread_list_response = crawler.fetch_tag_threadlist(tag_id, user_token, school_alias) # from_time=0
    if not (thread_list_response and 'list' in thread_list_response):
        _retry_task(r, task_dict, f"获取话题{tag_id}内帖子列表失败", max_retries=config.MAX_LIST_RETRIES)
        return

    thread_list = thread_list_response['list']
    data_handler.save_mx_threads(tag_id, thread_list)
    for thread in thread_list:
        if thread.get('thread_id'):
            dispatch_task(r, 'process_chain_b_final_details', {'thread_id': thread['thread_id'], 'p_time': thread.get('p_time'), 'user_token': user_token, 'school_alias': school_alias})
    tqdm.write(f"[Worker] 已为话题 {tag_id} 内的 {len(thread_list)} 个帖子分发子任务。")

def process_chain_b_final_details(r, payload, task_dict):
    """处理链B的最终步骤，现在接收 task_dict 用于重试"""
    thread_id, user_token, school_alias = payload['thread_id'], payload['user_token'], payload['school_alias']

    details_response = crawler.fetch_mx_thread_info(thread_id, user_token, school_alias)
    if not (details_response and 'detail' in details_response):
        _retry_task(r, task_dict, f"MX帖子{thread_id}详情为空/无效", max_retries=config.MAX_DETAIL_RETRIES)
        return
    
    post_detail = details_response['detail']
    data_handler.save_mx_threads(payload.get('tag_id'), [post_detail]) # 存入数据库
    
    t_sign = details_response.get('t_sign')
    if not t_sign: return
        
    all_comments = _fetch_all_comments(crawler.fetch_mx_comment_list, thread_id, t_sign, user_token, school_alias)
    if all_comments: data_handler.save_mx_comments(thread_id, all_comments)

# =================================================================
#  主任务调度器 和 进度条/主循环
# =================================================================
def process_master_task(task_json, r_conn):
    """主任务调度器，根据任务类型调用不同的处理链"""
    global processed_count
    task = json.loads(task_json)
    try:
        task_type, payload = task.get('type'), task.get('payload', {})
        task_handlers = {
            'process_chain_a': process_chain_a,
            'process_chain_b_start': process_chain_b_start,
            'process_chain_b_get_threads': process_chain_b_get_threads,
            'process_chain_b_final_details': process_chain_b_final_details
        }
        handler = task_handlers.get(task_type)
        if handler:
            handler(r_conn, payload, task)
        else:
            tqdm.write(f"[Worker] [警告] 未知任务类型: {task_type}")
    except Exception as e:
        tqdm.write(f"[Worker] [错误] 处理任务时发生未知异常: {e}")
        _retry_task(r_conn, task, str(e))
    finally:
        with threading.Lock():
            processed_count += 1

def update_progress_bar(r):
    """独立的进度条更新线程"""
    global pbar, processed_count
    pbar = tqdm(total=0, desc="[Worker] 等待任务", unit="个", bar_format="{l_bar}{bar}| {n}/{total_fmt} [{elapsed}]")
    last_known_total = 0
    while not stop_event.is_set():
        try:
            total_tasks_signal = r.get(config.REDIS_BATCH_TOTAL_KEY)
            if total_tasks_signal:
                current_total = int(total_tasks_signal)
                if current_total != last_known_total and current_total >= 0:
                    pbar.reset(total=current_total)
                    with threading.Lock(): processed_count = 0
                    last_known_total = current_total
                    if current_total > 0: pbar.set_description("[Worker] 处理新批次")
            
            remaining = r.llen(config.REDIS_QUEUE_NAME)
            pbar.total = max(pbar.total if pbar.total is not None else 0, processed_count + remaining)
            pbar.update(processed_count - pbar.n)
            pbar.set_postfix_str(f"队列剩余: {remaining}")
            
            if last_known_total > 0 and processed_count >= last_known_total and remaining == 0:
                pbar.set_description(f"[Worker] 批次({last_known_total}个)完成")
                r.publish(config.REDIS_DONE_CHANNEL, str(processed_count))
                last_known_total = -1
            time.sleep(1)
        except Exception: time.sleep(5)

def main():
    """Worker主函数，负责启动和管理线程池"""
    global stop_event
    data_handler.setup_all_databases()
    r = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
    
    progress_thread = threading.Thread(target=update_progress_bar, args=(r,), daemon=True)
    progress_thread.start()
    
    with ThreadPoolExecutor(max_workers=config.CONCURRENT_WORKERS) as executor:
        print(f"[Worker] 并发工人已启动 (并发数: {config.CONCURRENT_WORKERS})，等待生产者指令...")
        try:
            while not stop_event.is_set():
                signal = r.get(config.REDIS_CONTROL_SIGNAL_KEY)
                if signal and signal.decode('utf-8') == 'STOP':
                    print("\n[Worker] 收到停止指令，等待当前任务完成后退出...")
                    executor.shutdown(wait=True) # 等待线程池中所有任务完成
                    break
                
                task_tuple = r.brpop(config.REDIS_QUEUE_NAME, timeout=1)
                if task_tuple:
                    _, task_json_bytes = task_tuple
                    executor.submit(process_master_task, task_json_bytes, r)
        except KeyboardInterrupt: print("\n[Worker] 检测到Ctrl+C...")
        
    stop_event.set()
    if pbar: pbar.close()
    progress_thread.join()
    print("\n[Worker] 程序已退出。")

if __name__ == '__main__':
    main()
# zanao_climber/main.py

import time, json, redis, random
from datetime import datetime
from tqdm import tqdm
from zanao_climber import config, crawler, data_handler

def dispatch_task(r, task_type, payload):
    """分发任务到Redis队列"""
    task = {'type': task_type, 'payload': payload, 'retries': 0}
    r.lpush(config.REDIS_QUEUE_NAME, json.dumps(task))

def get_user_choice(prompt, choices=['y', 'n']):
    """获取用户输入，并验证"""
    while True:
        choice = input(prompt).strip().lower()
        if choice in choices: return choice
        print(f"无效输入，请输入 {'/'.join(choices)} 中的一个。")

def wait_for_workers_to_finish(r_pubsub, dispatched_count):
    """通过Redis Pub/Sub等待Worker完成一批任务"""
    if dispatched_count == 0:
        print("[生产者] 本批次无新任务，无需等待。")
        # 即使无任务，也发一个0信号，让进度条清零
        r_pubsub.set(config.REDIS_BATCH_TOTAL_KEY, 0)
        return
        
    print(f"\n[生产者] 已分发 {dispatched_count} 个任务，等待Worker处理完成...")
    pubsub = r_pubsub.pubsub()
    pubsub.subscribe(config.REDIS_DONE_CHANNEL)
    with tqdm(total=dispatched_count, desc="[生产者] 等待Worker") as pbar:
        while pbar.n < dispatched_count:
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message['type'] == 'message':
                try:
                    processed = int(message['data'])
                    pbar.update(processed - pbar.n)
                except (ValueError, TypeError): pass
    tqdm.write("\n[生产者] 收到Worker完成信号！")
    pubsub.unsubscribe(); pubsub.close()

# =============================================================
#  爬取链 A: 普通帖子
# =============================================================
def fetch_and_dispatch_chain_a(r, start_ts, end_ts):
    print(f"\n[生产者] 扫描普通帖子: {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M')} -> {datetime.fromtimestamp(end_ts).strftime('%Y-%m-%d %H:%M')}")
    all_ids, next_from = set(), end_ts
    
    token = random.choice(config.USER_TOKENS)
    print(f"[生产者] 本轮扫描使用Token: ...{token[-10:]}")
    
    with tqdm(total=int(end_ts - start_ts), desc="[生产者] 扫描进度", unit_scale=True, unit="s", leave=True) as pbar:
        for _ in range(config.MAX_PAGES_TO_FETCH):
            ids_times, earliest = crawler.fetch_post_list(token, config.SCHOOL_ALIAS, from_time=next_from)
            if not ids_times or earliest is None: pbar.update(pbar.total - pbar.n); tqdm.write("\n[生产者] 已达最早帖子或获取失败。"); break
            
            progress = end_ts - earliest
            pbar.update(progress - pbar.n if progress > pbar.n else 0)
            
            new_in_page = {(pid, pt) for pid, pt in ids_times if pt >= start_ts}
            if not new_in_page and next_from != end_ts: tqdm.write("\n[生产者] 本页已无目标时间范围内的帖子。"); break
            
            all_ids.update(new_in_page)
            if earliest < start_ts: break
            next_from = earliest
            time.sleep(config.PRODUCER_BASE_DELAY + random.uniform(0, config.PRODUCER_RANDOM_DELAY))

    if not all_ids: print("[生产者] 在指定时间范围内未发现新帖。"); return 0
    
    conn = data_handler.get_posts_db_conn()
    ids_to_check = [pid for pid, _ in all_ids]
    placeholders = ','.join('?'*len(ids_to_check))
    existing_ids = {r['thread_id'] for r in conn.execute(f"SELECT thread_id FROM posts WHERE thread_id IN ({placeholders})", ids_to_check).fetchall()} if ids_to_check else set()
    conn.close()
    
    tasks = [(pid, pt) for pid, pt in all_ids if pid not in existing_ids]
    if tasks:
        total = len(tasks); print(f"\n[生产者] 发现 {total} 个新帖，准备分发...")
        r.set(config.REDIS_BATCH_TOTAL_KEY, total, ex=3600)
        for pid, pt in tqdm(tasks, desc="[生产者] 分发任务"):
            payload = {'post_id': pid, 'post_time': pt, 'user_token': token, 'school_alias': config.SCHOOL_ALIAS}
            dispatch_task(r, 'process_chain_a', payload)
        return total
    else: print("[生产者] 发现的所有帖子均已存在于数据库中。"); r.set(config.REDIS_BATCH_TOTAL_KEY, 0, ex=3600); return 0

def run_posts_history_mode(r_command, r_pubsub):
    print("\n--- 模式: 爬取历史普通帖子 ---")
    start_str = input("请输入开始日期 (yyyy-mm-dd): ").strip()
    end_str = input("请输入结束日期 (yyyy-mm-dd): ").strip()
    try:
        start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp())
        end_ts = int(datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp())
    except ValueError: print("错误：日期格式。"); return
    
    total_hours = (end_ts - start_ts) / 3600
    chunk_h_in = input(f"总跨度约{total_hours:.1f}小时, 请输入分块小时数 (回车不分块, 可输入小数如0.5): ").strip()
    try: chunk_h = total_hours + 1 if chunk_h_in == "" else float(chunk_h_in)
    except ValueError: print("无效的小时数。"); return
    
    chunk_s, curr_start = int(chunk_h * 3600), end_ts
    while curr_start > start_ts:
        r_command.set(config.REDIS_CONTROL_SIGNAL_KEY, 'CONTINUE')
        curr_end = curr_start
        curr_start = max(curr_start - chunk_s, start_ts)
        dispatched = fetch_and_dispatch_chain_a(r_command, curr_start, curr_end)
        wait_for_workers_to_finish(r_pubsub, dispatched)
        if curr_start <= start_ts: print("\n[生产者] 所有分块已爬取！"); break
        if get_user_choice("是否继续上一分块? (y/n): ") == 'n': break

def run_posts_incremental_mode(r_command, r_pubsub):
    print("\n--- 模式: 增量监控普通帖子 (按 Ctrl+C 退出) ---")
    while True:
        try:
            r_command.set(config.REDIS_CONTROL_SIGNAL_KEY, 'CONTINUE')
            conn = data_handler.get_posts_db_conn()
            latest_record = conn.execute('SELECT MAX(create_time_ts) as max_ts FROM posts').fetchone()
            conn.close()
            start_ts = int(time.time()) - 86400
            if latest_record and latest_record['max_ts'] is not None:
                start_ts = latest_record['max_ts'] + 1
            
            print(f"\n[智能检测] 从 {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')} 开始扫描...")
            dispatched = fetch_and_dispatch_chain_a(r_command, start_ts, int(time.time()))
            wait_for_workers_to_finish(r_pubsub, dispatched)
            
            print("\n[生产者] 本轮扫描处理完成。")
            if get_user_choice(f"是否在 {config.INCREMENTAL_SCAN_INTERVAL}秒后开始下一轮? (y/n): ") == 'n': break
            for _ in tqdm(range(config.INCREMENTAL_SCAN_INTERVAL), desc="[生产者] 等待中"): time.sleep(1)
        except KeyboardInterrupt: break
        except Exception as e:
            print(f"\n[生产者] [严重错误] 增量模式异常: {e}")
            if get_user_choice("是否在60秒后重试? (y/n): ") == 'n': break; time.sleep(60)

# =============================================================
#  爬取链 B: 跨校区话题
# =============================================================
def fetch_and_dispatch_chain_b(r, start_timestamp, end_timestamp):
    tqdm.write(f"\n[生产者] 扫描跨校区话题内帖子...")
    current_token, school_alias = random.choice(config.USER_TOKENS), config.SCHOOL_ALIAS
    tqdm.write("[生产者] 步骤1: 更新热门话题列表...")
    hot_tags_data = crawler.fetch_hot_tags(current_token, school_alias)
    if not (hot_tags_data and 'list' in hot_tags_data): tqdm.write("[生产者] [错误] 无法获取热门话题列表。"); return 0
    hot_tags_list = hot_tags_data['list']
    data_handler.save_hot_tags(hot_tags_list); tqdm.write(f"已更新 {len(hot_tags_list)} 个热门话题。")

    all_new_mx_threads = set()
    for tag in tqdm(hot_tags_list, desc="[生产者] 总览各话题"):
        tag_id, tag_name = tag.get('tag_id'), tag.get('name', '未知话题')
        if not tag_id: continue
        tqdm.write(f"  -> 开始扫描话题: '{tag_name[:20]}'")
        next_from_time = end_timestamp
        last_page_ids_mx = set()
        
        for page_num in range(1, (config.MAX_PAGES_TO_FETCH or 50) + 1):
            ids_and_times, earliest_time = crawler.fetch_tag_threadlist(tag_id, current_token, school_alias, from_time=next_from_time)
            if not ids_and_times or earliest_time is None:
                tqdm.write(f"    - 第 {page_num} 页: 未获取到数据，此话题扫描结束。"); break
            
            current_page_ids_mx = {pid for pid, _ in ids_and_times}
            if current_page_ids_mx == last_page_ids_mx:
                tqdm.write(f"    - [警告] 第 {page_num} 页: 检测到重复页面，强制跳出。"); break
            last_page_ids_mx = current_page_ids_mx
            
            tqdm.write(f"    - 第 {page_num} 页: 获取到 {len(ids_and_times)} 个帖子，最早时间: {datetime.fromtimestamp(earliest_time).strftime('%H:%M:%S')}")

            for thread_id, p_time in ids_and_times:
                if p_time >= start_timestamp and p_time <= end_timestamp:
                    all_new_mx_threads.add((thread_id, p_time, tag_id))
            
            if earliest_time < start_timestamp:
                tqdm.write(f"    - 已扫描到早于起始时间的帖子，此话题扫描结束。"); break
            
            if earliest_time == next_from_time: next_from_time = earliest_time - 1
            else: next_from_time = earliest_time
            
            time.sleep(0.5)

    if not all_new_mx_threads:
        print("\n[生产者] 在所有话题的目标时间范围内，均未发现新的帖子。")
        return 0
        
    # --- 后续的去重和分发逻辑完全正确，保持不变 ---
    conn = data_handler.get_mx_db_conn()
    ids_to_check = [tid for tid, _, _ in all_new_mx_threads]
    if not ids_to_check: existing_ids = set()
    else:
        placeholders = ','.join('?'*len(ids_to_check))
        existing_ids = {r['thread_id'] for r in conn.execute(f"SELECT thread_id FROM mx_threads WHERE thread_id IN ({placeholders})", ids_to_check).fetchall()}
    conn.close()
    
    tasks_to_dispatch = [(tid, pt, tagid) for tid, pt, tagid in all_new_mx_threads if tid not in existing_ids]
    if tasks_to_dispatch:
        total_new_tasks = len(tasks_to_dispatch)
        print(f"\n[生产者] 发现 {total_new_tasks} 个新MX帖，准备分发...")
        r.set(config.REDIS_BATCH_TOTAL_KEY, total_new_tasks, ex=3600)
        for thread_id, p_time, tag_id in tqdm(tasks_to_dispatch, desc="[生产者] 分发MX任务"):
            payload = {'thread_id': thread_id, 'p_time': p_time, 'tag_id': tag_id, 'user_token': current_token, 'school_alias': school_alias}
            dispatch_task(r, 'process_chain_b_final_details', payload)
        return total_new_tasks
    else:
        print("\n[生产者] 发现的跨校区帖子均已在数据库中。")
        r.set(config.REDIS_BATCH_TOTAL_KEY, 0, ex=3600)
        return 0

def run_mx_history_mode(r_command, r_pubsub):
    print("\n--- 模式: 爬取历史跨校区帖子 ---")
    start_str = input("请输入开始日期 (yyyy-mm-dd): ").strip()
    end_str = input("请输入结束日期 (yyyy-mm-dd): ").strip()
    try:
        start_ts = int(datetime.strptime(start_str, "%Y-%m-%d").timestamp())
        end_ts = int(datetime.strptime(end_str, "%Y-%m-%d").replace(hour=23, minute=59, second=59).timestamp())
    except ValueError: print("错误：日期格式不正确。"); return
    
    dispatched = fetch_and_dispatch_chain_b(r_command, start_ts, end_ts)
    wait_for_workers_to_finish(r_pubsub, dispatched)
    print("\n[生产者] 历史跨校区帖子任务已处理完毕！")

def run_mx_incremental_mode(r_command, r_pubsub):
    print("\n--- 模式: 增量监控跨校区帖子 (按 Ctrl+C 退出) ---")
    while True:
        try:
            r_command.set(config.REDIS_CONTROL_SIGNAL_KEY, 'CONTINUE')
            conn = data_handler.get_mx_db_conn()
            latest = conn.execute('SELECT MAX(create_time_ts) as max_ts FROM mx_threads').fetchone()
            conn.close()
            start_ts = int(time.time()) - 86400
            if latest and latest['max_ts'] is not None: start_ts = latest['max_ts'] + 1
            
            print(f"\n[智能检测] 从 {datetime.fromtimestamp(start_ts).strftime('%Y-%m-%d %H:%M:%S')} 开始扫描跨校区帖子...")
            dispatched = fetch_and_dispatch_chain_b(r_command, start_ts, int(time.time()))
            wait_for_workers_to_finish(r_pubsub, dispatched)
            
            print("\n[生产者] 本轮扫描处理完成。")
            if get_user_choice(f"是否在 {config.INCREMENTAL_SCAN_INTERVAL}秒后开始下一轮? (y/n): ") == 'n': break
            for _ in tqdm(range(config.INCREMENTAL_SCAN_INTERVAL), desc="[生产者] 等待中"): time.sleep(1)
        except KeyboardInterrupt: break
        except Exception as e:
            print(f"\n[生产者] [严重错误] 增量模式异常: {e}")
            if get_user_choice("是否在60秒后重试? (y/n): ") == 'n': break; time.sleep(60)

def main():
    """主启动器"""
    data_handler.setup_all_databases()
    # 使用 decode_responses=False 的连接用于普通命令，避免Pub/Sub的兼容问题
    r_command = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB)
    # 使用 decode_responses=True 的连接专门用于Pub/Sub
    r_pubsub = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=config.REDIS_DB, decode_responses=True)
    
    print("[生产者] 初始化系统状态：向Worker发送'CONTINUE'指令...")
    r_command.set(config.REDIS_CONTROL_SIGNAL_KEY, 'CONTINUE')
    r_command.delete(config.REDIS_BATCH_TOTAL_KEY)
    
    try:
        while True:
            print("\n欢迎使用Zanao多功能智能爬虫系统"); print("="*34)
            print("--- 普通帖子模块 (DB1) ---"); print("  1. 爬取历史帖子"); print("  2. 启动增量监控")
            print("\n--- 跨校区话题模块 (DB2) ---"); print("  3. 爬取历史话题内帖子"); print("  4. 启动增量监控")
            print("\nq. 退出程序")
            mode = get_user_choice("请选择运行模式 (1/2/3/4/q): ", ['1', '2', '3', '4', 'q'])
            
            if mode == '1': run_posts_history_mode(r_command, r_pubsub)
            elif mode == '2': run_posts_incremental_mode(r_command, r_pubsub)
            elif mode == '3': run_mx_history_mode(r_command, r_pubsub)
            elif mode == '4': run_mx_incremental_mode(r_command, r_pubsub)
            elif mode == 'q': break
            
            if get_user_choice("\n是否返回主菜单? (y/n): ") == 'n': break
    finally:
        print("\n[生产者] 正在通知Worker停止工作...")
        r_command.set(config.REDIS_CONTROL_SIGNAL_KEY, 'STOP', ex=60)
        print("[生产者] 程序退出。")

if __name__ == "__main__":
    main()

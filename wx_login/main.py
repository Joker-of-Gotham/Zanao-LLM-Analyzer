import sys
import os
import json
import time
import argparse
import shutil
from pprint import pprint
from datetime import datetime
from pysqlcipher3 import dbapi2 as sqlite

# 动态设置项目根路径，确保无论从哪里运行都能找到模块
script_path = os.path.abspath(__file__)
wx_login_dir = os.path.dirname(script_path)
project_root = os.path.dirname(wx_login_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入本地模块
from wx_login.wx_info_handler import get_wx_info
from group_chat_getter.exporter import (
    decrypt_database, merge_msg_databases,
    get_all_group_chats, get_messages_for_chat,
    export_to_txt, export_to_csv
)

def get_contact_nickname(contact_db_path, wxid):
    conn = sqlite.connect(contact_db_path)
    cur = conn.cursor()
    cur.execute("SELECT NickName FROM Contact WHERE UserName = ?", (wxid,))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0]
    return wxid  # 如果没找到，返回 wxid

def safe_get_wx_info(offsets, wechat_path, retries=3, delay=0.5):
    """带重试的微信信息提取"""
    for attempt in range(1, retries + 1):
        all_info = get_wx_info(offsets, wechat_files_path=wechat_path)
        valid = [u for u in all_info if u.get('key') and u.get('wx_dir')]
        if valid:
            return valid
        print(f"[重试] 第 {attempt} 次未拿到信息, {delay}s 后再试...")
        time.sleep(delay)
    return []

def run_main():
    print("\n[1] 正在加载微信版本偏移数据...")
    WX_OFFS_PATH = os.path.join(project_root, "wx_login/WX_OFFS.json")
    if not os.path.exists(WX_OFFS_PATH):
        print(f"[x] 错误: 找不到偏移文件 {WX_OFFS_PATH}")
        sys.exit(1)
    with open(WX_OFFS_PATH, 'r', encoding='utf-8') as f:
        wx_offs = json.load(f)
    print(" [√] 偏移数据加载成功。")

    print("\n[2] 开始从内存中读取微信信息...\n    (请确保微信客户端正在运行)\n")
    print("[调试] 当前 WX_OFFS.json 支持的微信版本：")
    print(json.dumps(list(wx_offs.keys()), indent=2, ensure_ascii=False))

    parser = argparse.ArgumentParser(description="微信群聊导出工具")
    parser.add_argument('--chat-index', type=int, help='群聊编号，从1开始')
    parser.add_argument('--start', type=str, help='开始日期 YYYY-MM-DD')
    parser.add_argument('--end', type=str, help='结束日期 YYYY-MM-DD')
    parser.add_argument('--format', choices=['txt', 'csv'], default='txt', help='导出格式')
    parser.add_argument('--wechat-path', type=str,
        default=os.path.join(os.path.expanduser('~'), 'Documents', 'WeChat Files'),
        help='WeChat Files 根目录')
    args = parser.parse_args()

    users = safe_get_wx_info(wx_offs, args.wechat_path)
    if not users:
        print("[x] 无有效微信登录信息, 请确保微信正在运行并使用管理员权限启动脚本.")
        sys.exit(1)

    print("\n [√] 最终成功获取到 {} 个有效微信用户的信息！\n".format(len(users)))
    print("=" * 50)
    print("               用户信息详情")
    print("=" * 50)
    pprint(users[0])
    print("=" * 50)

    user = users[0]
    key = user['key']
    wx_dir = os.path.normpath(user['wx_dir'])

    # 解密主数据库
    temp_dir = os.path.join(project_root, 'temp_decrypted')
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir)

    print("\n[3] 正在解密 MicroMsg.db 数据库...")
    micro_db = os.path.join(wx_dir, 'Msg', 'MicroMsg.db')
    dec_micro = os.path.join(temp_dir, 'MicroMsg.db')
    if not decrypt_database(key, micro_db, dec_micro):
        print("[x] 解密 MicroMsg.db 失败")
        sys.exit(1)
    print(" [√] MicroMsg.db 解密成功")

    print("\n[4] 正在解密 MSG 数据库文件...")
    msg_dir = os.path.join(wx_dir, 'Msg', 'Multi')
    dec_list = []
    if os.path.exists(msg_dir):
        for fn in sorted(os.listdir(msg_dir)):
            if fn.startswith('MSG') and fn.endswith('.db'):
                src = os.path.join(msg_dir, fn)
                dst = os.path.join(temp_dir, fn)
                if decrypt_database(key, src, dst):
                    dec_list.append(dst)
    merged = os.path.join(temp_dir, 'merged_msg.db')
    if not merge_msg_databases(dec_list, merged):
        print("[x] 合并消息数据库失败")
        sys.exit(1)
    print(" [√] 合并成功，共合并 {} 个数据库".format(len(dec_list)))

    print("\n[5] 正在获取群聊列表...")
    chats = get_all_group_chats(dec_micro)
    if not chats:
        print("[x] 未找到任何群聊.")
        shutil.rmtree(temp_dir)
        sys.exit(1)
    print(" [√] 共找到 {} 个群聊".format(len(chats)))

    # 选择群聊
    if args.chat_index:
        idx = args.chat_index
    else:
        print("\n--- 可用群聊列表 ---")
        for i, c in enumerate(chats, 1):
            print(f"  [{i}] {c[1]}")
        idx = int(input("请输入群聊编号: "))
    if idx < 1 or idx > len(chats):
        print(f"[x] 无效的群聊编号: {idx}")
        sys.exit(1)
    selected = chats[idx - 1]

    # 时间范围
    if args.start and args.end:
        try:
            sd = datetime.strptime(args.start, '%Y-%m-%d')
            ed = datetime.strptime(args.end, '%Y-%m-%d').replace(hour=23, minute=59, second=59)
        except ValueError:
            print("[x] 日期格式错误, 应为 YYYY-MM-DD")
            sys.exit(1)
    else:
        sd = datetime.strptime(input('开始日期 YYYY-MM-DD: '), '%Y-%m-%d')
        ed = datetime.strptime(input('结束日期 YYYY-MM-DD: '), '%Y-%m-%d').replace(hour=23, minute=59, second=59)
    start_ts, end_ts = int(sd.timestamp()), int(ed.timestamp())

    print("\n[6] 正在查询聊天记录...")
    msgs = get_messages_for_chat(merged, selected[0], start_ts, end_ts)
    print(f"[√] 成功查询到 {len(msgs)} 条消息")

    # 1. 设置包含 Contact 表的数据库路径
    # 你需要将 "path/to/MicroMsg.db" 替换为正确的路径
    contact_db_path = dec_micro

    # 2. 遍历消息，为每条消息添加昵称
    for msg in msgs:
        sender_wxid = msg['StrTalker']
        msg['sender_nickname'] = get_contact_nickname(contact_db_path, sender_wxid)

    out_root = os.path.join(project_root, 'data/original_data')
    os.makedirs(out_root, exist_ok=True)
    safe_name = ''.join(c for c in selected[1] if c.isalnum()) or 'Chat'
    base = f"{safe_name}_{sd.date()}_to_{ed.date()}"
    out_path = os.path.join(out_root, base + ('.txt' if args.format=='txt' else '.csv'))

    print(f"\n[7] 正在导出消息到 {out_path}...")
    if args.format == 'txt':
        export_to_txt(msgs, out_path, selected)
    else:
        export_to_csv(msgs, out_path, selected)
    print(" [√] 导出完成！")

    shutil.rmtree(temp_dir)
    print("\n[完成] 所有流程已完成，临时文件已清理。")

if __name__ == '__main__':
    run_main()

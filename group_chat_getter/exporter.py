# group_chat_getter/exporter.py

import os
import shutil
import csv
from pysqlcipher3 import dbapi2 as sqlite
from datetime import datetime
from Cryptodome.Cipher import AES
import hmac
import hashlib
from .decryption_module import decryption as pywxd_decrypt_mod

SQLITE_FILE_HEADER = b"SQLite format 3\x00"
KEY_SIZE = 32
PAGE_SIZE = 4096
RESERVE_SIZE = 48  # 每页末尾的保留区字节数

SQLITE_FILE_HEADER = b"SQLite format 3\x00"

def decrypt_database(key: str, db_path: str, out_path: str) -> bool:
    """
    使用 pywxdump 的解密算法或复制未加密的 SQLite 数据库
    """
    if not os.path.exists(db_path):
        print(f"  [解密失败] 数据库不存在: {db_path}")
        return False

    # 检查文件头判断是否为明文 SQLite
    try:
        with open(db_path, 'rb') as f:
            header = f.read(len(SQLITE_FILE_HEADER))
    except Exception as e:
        print(f"  [解密失败] 无法读取文件: {e}")
        return False

    if header == SQLITE_FILE_HEADER:
        try:
            shutil.copyfile(db_path, out_path)
            print(f"  [复制成功] 未加密库 -> {os.path.basename(out_path)}")
            return True
        except Exception as e:
            print(f"  [复制失败] {e}")
            return False

    # 尝试使用 pywxdump 的解密函数
    print(f"  正在使用 pywxdump 解密 {os.path.basename(db_path)}...")
    code, ret = pywxd_decrypt_mod.decrypt(key=key, db_path=db_path, out_path=out_path)

    if code:
        print(f"  [解密成功] -> {os.path.basename(out_path)}")
        return True
    else:
        print(f"  [解密失败] {ret}")
        if os.path.exists(out_path):
            os.remove(out_path)
        return False
# --- 合并函数 ---

def merge_msg_databases(db_list: list, merged_db_path: str) -> bool:
    """合并多个解密后的 MSG 数据库到一个"""
    if not db_list:
        return False
    # 复制第一个
    shutil.copyfile(db_list[0], merged_db_path)
    conn = sqlite.connect(merged_db_path)
    cur = conn.cursor()
    for idx, f in enumerate(db_list[1:], start=1):
        try:
            cur.execute(f"ATTACH DATABASE '{f}' AS db{idx}")
            cur.execute(f"INSERT OR IGNORE INTO MSG SELECT * FROM db{idx}.MSG")
            cur.execute(f"DETACH DATABASE db{idx}")
        except Exception as e:
            print(f"  [合并警告] 合并 {os.path.basename(f)} 时: {e}")
    conn.commit()
    conn.close()
    print("\n[合并成功] 所有聊天记录已合并。")
    return True

# --- 查询与导出 ---

def get_all_group_chats(db_path):
    conn = sqlite.connect(db_path)
    cursor = conn.cursor()

    # 将原来的查询语句替换成这段，使用 JOIN 来获取正确的群聊名称
    query = """
    SELECT T1.ChatRoomName, T2.NickName
    FROM ChatRoom AS T1
    JOIN Contact AS T2
    ON T1.ChatRoomName = T2.UserName;
    """
    
    cursor.execute(query)
    groups = cursor.fetchall()

    conn.close()
    return groups


def get_messages_for_chat(db_path: str, chat_id: str, start_ts: int, end_ts: int) -> list:
    msgs = []
    try:
        conn = sqlite.connect(db_path)
        conn.row_factory = sqlite.Row
        cur = conn.cursor()

        # 第一步：根据 chat_id (即群聊的 UsrName) 从 Name2ID 表中获取 TalkerId
        cur.execute("SELECT rowid FROM Name2ID WHERE UsrName = ?", (chat_id,))
        row = cur.fetchone()
        if not row:
            print(f"[错误] 未在 Name2ID 表中找到群聊 {chat_id} 的 ID")
            return []
        
        tid = row[0]

        # 第二步：使用 TalkerId 查询 MSG 表，并【恢复时间限制】
        cur.execute(
            "SELECT StrTalker, StrContent, CreateTime, Type, IsSender"
            " FROM MSG WHERE TalkerId = ? AND CreateTime BETWEEN ? AND ?" # <-- 时间限制已恢复
            " ORDER BY CreateTime ASC",
            (tid, start_ts, end_ts)  # <-- 参数也已恢复为3个
        )
        for r in cur.fetchall():
            msgs.append(dict(r))
        conn.close()

    except Exception as e:
        print(f"[错误] 查询消息失败: {e}")
    return msgs


# 这是最终的、支持备注名的函数，请用它替换 exporter.py 中的旧版本
def get_all_chats_and_contacts(db_path: str) -> list:
    """从 MicroMsg.db 中获取所有有意义的聊天会话列表（文件助手、好友、群聊），并优先使用备注名"""
    all_chats = []
    try:
        conn = sqlite.connect(db_path)
        cur = conn.cursor()

        # [修改点 1] 在查询语句中，增加了 Remark 字段
        query = """
            SELECT UserName, NickName, Remark FROM Contact 
            WHERE (UserName LIKE '%@chatroom' OR Type = 3 OR UserName = 'filehelper')
            AND NickName != '' 
            AND UserName NOT LIKE 'gh_%'
        """
        cur.execute(query)
        
        results = cur.fetchall()
        conn.close()

        file_helper = None
        friends = []
        groups = []

        for row in results:
            # [修改点 2] 接收三个返回值：用户名、昵称、备注
            username, nickname, remark = row[0], row[1], row[2]

            if username == 'filehelper':
                file_helper = (username, "文件传输助手")
                
            elif username.endswith('@chatroom'):
                # 群聊直接使用群聊名称 (NickName)
                groups.append((username, nickname))
                
            else: 
                # [修改点 3] 核心逻辑：如果备注 (remark) 存在，就用备注；否则，用昵称 (nickname)
                display_name = remark if remark else nickname
                friends.append((username, display_name))
        
        # 后续的排序和组合逻辑保持不变
        friends.sort(key=lambda x: x[1])
        groups.sort(key=lambda x: x[1])

        if file_helper:
            all_chats.append(file_helper)
        
        all_chats.extend(friends)
        all_chats.extend(groups)

        return all_chats

    except Exception as e:
        print(f"[错误] 获取所有聊天列表失败: {e}")
        return []


def export_to_txt(messages: list, output_path: str, chat_info: dict) -> None:
    print(f"\n[导出TXT] -> {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("="*50 + "\n")
        f.write(f"群聊: {chat_info[1]} ({chat_info[0]})\n")
        f.write(f"导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n\n")
        for m in messages:
            ts = datetime.fromtimestamp(m['CreateTime']).strftime('%Y-%m-%d %H:%M:%S')
            snd = "我" if m['IsSender']==1 else m['StrTalker']
            cnt = m.get('StrContent','')
            if m['Type']==1:
                f.write(f"[{ts}] {snd}: {cnt}\n")
            else:
                f.write(f"[{ts}] {snd}: [非文本消息, Type={m['Type']}]\n")
    print("[TXT导出完成]")


def export_to_csv(messages: list, output_path: str, chat_info: dict) -> None:
    print(f"\n[导出CSV] -> {output_path}")
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['CreateTime','ReadableTime','Sender','Content','Type','IsSender'])
        for m in messages:
            rt = datetime.fromtimestamp(m['CreateTime']).strftime('%Y-%m-%d %H:%M:%S')
            writer.writerow([
                m['CreateTime'], rt,
                '我' if m['IsSender']==1 else m['StrTalker'],
                m.get('StrContent',''),
                m.get('Type'), m.get('IsSender')
            ])
    print("[CSV导出完成]")
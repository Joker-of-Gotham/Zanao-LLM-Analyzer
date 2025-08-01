# group_chat_getter/interactive_cli.py

import os
import shutil
from datetime import datetime

# 从同级目录的 exporter 模块中导入所有核心函数
from . import exporter
# 从 pywxdump 项目中导入获取数据库路径的函数
from pywxdump.wx_core.wx_info import get_core_db
# 从本地 decryption_module 文件夹中导入 get_key 函数
from .decryption_module.decryption import get_key as pywxd_get_key

def start_session(key: str, wx_dir: str, output_root_path: str):
    """
    启动交互式导出会话的公共入口函数
    :param key: 微信数据库密钥
    :param wx_dir: 微信用户数据目录 (例如 .../WeChat Files/wxid_xxxx)
    :param output_root_path: 导出文件的根目录 (例如 .../完整工程实现/)
    """
    print("\n--- 准备数据：开始解密数据库 (可能需要一点时间) ---")
    
    # 1. 准备路径
    project_root = os.path.dirname(__file__)
    temp_decrypted_dir = os.path.join(project_root, "temp_decrypted")
    if os.path.exists(temp_decrypted_dir):
        shutil.rmtree(temp_decrypted_dir)
    os.makedirs(temp_decrypted_dir)

    # 2. 获取所有需要解密的数据库路径
    print("[*] 获取数据库文件列表...")
    code, wxdb_paths_info = get_core_db(wx_dir)
    if not code:
        print(f"[错误] 获取数据库路径失败: {wxdb_paths_info}")
        shutil.rmtree(temp_decrypted_dir)
        return
    
    # 3. 批量解密数据库
    decrypted_msg_dbs = []
    decrypted_micro_db = None

    for db_info in wxdb_paths_info:
        db_path = db_info['db_path']
        db_type = db_info['db_type']
        
        # 针对每个数据库，创建新的解密文件路径
        out_path = os.path.join(temp_decrypted_dir, f"de_{db_type}_{os.path.basename(db_path)}")
        
        print(f"[*] 正在处理 {os.path.basename(db_path)}...")
        if exporter.decrypt_database(key, db_path, out_path):
            if 'MicroMsg' in db_type:
                decrypted_micro_db = out_path
            elif 'MSG' in db_type:
                decrypted_msg_dbs.append(out_path)
            else:
                print(f"[提示] 数据库 {db_path} 类型未知，跳过。")
    
    # 确保 MicroMsg.db 已经解密
    if not decrypted_micro_db:
        print("[错误] 未能成功解密 MicroMsg.db，无法继续。")
        shutil.rmtree(temp_decrypted_dir)
        return

    # 4. 合并所有 MSG.db
    print("\n--- 合并聊天记录数据库 ---")
    merged_db_path = os.path.join(temp_decrypted_dir, "merged_msg.db")
    if not exporter.merge_msg_databases(decrypted_msg_dbs, merged_db_path):
        print("[错误] 聊天记录数据库合并失败，无法继续。")
        shutil.rmtree(temp_decrypted_dir)
        return

    # 5. 获取并展示群聊列表
    try:
        group_chats = exporter.get_all_group_chats(decrypted_micro_db)
    except Exception as e:
        print(f"\n[致命错误] 获取群聊列表时发生异常: {e}")
        input("程序已暂停。请立即检查 'temp_decrypted' 目录。按回车键继续...")
        shutil.rmtree(temp_decrypted_dir)
        return

    if not group_chats:
        print("\n[提示] 未能找到任何群聊。")
        input("程序已暂停。请立即检查 'temp_decrypted' 目录。按回车键继续...")
        shutil.rmtree(temp_decrypted_dir)
        return
        
    print("\n--- 请选择要导出的群聊 ---")
    for i, chat in enumerate(group_chats):
        print(f"  [{i+1}] {chat['nickname']}")
    
    while True:
        try:
            choice = int(input("请输入群聊编号: "))
            if 1 <= choice <= len(group_chats):
                selected_chat = group_chats[choice - 1]
                break
            else:
                print("输入无效，请输入列表中的编号。")
        except ValueError:
            print("输入无效，请输入数字。")
            
    print(f"\n[+] 已选择群聊: {selected_chat['nickname']} ({selected_chat['wxid']})")
    print("[*] 正在查询消息...")
    
    # 假设查询所有消息
    messages = exporter.get_messages_for_chat(merged_db_path, selected_chat['wxid'], 0, float('inf'))
    
    if not messages:
        print("[提示] 未找到该群聊的任何消息。")
        shutil.rmtree(temp_decrypted_dir)
        return

    # 创建输出目录
    output_dir = os.path.join(output_root_path, f"chat_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(output_dir, exist_ok=True)

    # 导出文件
    exporter.export_to_txt(messages, os.path.join(output_dir, f"{selected_chat['nickname']}.txt"), selected_chat)
    exporter.export_to_csv(messages, os.path.join(output_dir, f"{selected_chat['nickname']}.csv"), selected_chat)
    
    print("\n[+] 导出完成！")
    
    # 清理临时文件
    shutil.rmtree(temp_decrypted_dir)
    print("[*] 清理临时文件完成。")
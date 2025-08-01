# wx_login/core_utils/common_utils.py

import os
import psutil
import hmac
import hashlib
from ._loger import wx_core_error

CORE_DB_TYPE = ["MSG", "MicroMsg", "MediaMSG", "FTSMSG", "Sns", "Emotion", "Favorite"]

def get_process_list():
    process_list = []
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            process_list.append((proc.info['pid'], proc.info['name']))
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return process_list

def get_memory_maps(pid):
    try:
        process = psutil.Process(pid)
        # 关键修复：使用 grouped=False 获取包含基地址的详细信息
        return process.memory_maps(grouped=False)
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return []

def get_process_exe_path(pid):
    try:
        process = psutil.Process(pid)
        return process.exe()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return ""

@wx_core_error
def get_file_version_info(file_path):
    if not os.path.exists(file_path):
        return None
    try:
        from win32api import GetFileVersionInfo, LOWORD, HIWORD
        info = GetFileVersionInfo(file_path, "\\")
        ms = info['FileVersionMS']
        ls = info['FileVersionLS']
        version = f"{HIWORD(ms)}.{LOWORD(ms)}.{HIWORD(ls)}.{LOWORD(ls)}"
        return version
    except Exception:
        return None

def get_exe_bit(exe_path):
    if not os.path.exists(exe_path):
        return 0
    with open(exe_path, 'rb') as f:
        f.seek(60)
        e_lfanew = f.read(4)
        f.seek(int.from_bytes(e_lfanew, byteorder='little') + 4)
        if f.read(2) == b'\x86\x64':
            return 64
        else:
            return 32

@wx_core_error
def verify_key(key, wx_db_path):
    """
    验证key是否正确 - 已修复类型转换问题
    """
    if not key or not wx_db_path or not os.path.exists(wx_db_path):
        return False
        
    # ==================== 关键修复：确保 key 是字节串 ====================
    # get_key_by_offs 返回的是十六进制字符串，我们必须先把它转换回字节
    if isinstance(key, str):
        try:
            key = bytes.fromhex(key)
        except (ValueError, TypeError):
            # 如果转换失败，说明 key 格式不对
            return False
    # =====================================================================

    KEY_SIZE = 32
    DEFAULT_PAGESIZE = 4096
    DEFAULT_ITER = 64000
    
    try:
        with open(wx_db_path, "rb") as file:
            blist = file.read(5000)
    except Exception:
        return False
        
    salt = blist[:16]
    
    # 现在 key, salt 都是正确的字节串类型
    pk = hashlib.pbkdf2_hmac("sha1", key, salt, DEFAULT_ITER, KEY_SIZE)
    
    first = blist[16:DEFAULT_PAGESIZE]
    mac_salt = bytes([(salt[i] ^ 58) for i in range(16)])
    
    # pk 已经是字节串，可以直接使用
    mac_pk = hashlib.pbkdf2_hmac("sha1", pk, mac_salt, 2, KEY_SIZE)
    
    hash_mac = hmac.new(mac_pk, first[:-32], hashlib.sha1)
    hash_mac.update(b'\x01\x00\x00\x00')
    
    if hash_mac.digest() == first[-32:-12]:
        return True
        
    return False
# wx_login/core_utils/__init__.py
from ._loger import wx_core_loger
from .common_utils import wx_core_error, verify_key, get_exe_bit
from .memory_search import search_memory
import os

import psutil
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
        # ==================== 关键修复：确保返回详细信息 ====================
        return process.memory_maps(grouped=False)
        # ================================================================
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
    if not os.path.exists(file_path): return None
    try:
        from win32api import GetFileVersionInfo, LOWORD, HIWORD
        info = GetFileVersionInfo(file_path, "\\")
        ms, ls = info['FileVersionMS'], info['FileVersionLS']
        return f"{HIWORD(ms)}.{LOWORD(ms)}.{HIWORD(ls)}.{LOWORD(ls)}"
    except Exception:
        return None

# 原始 wx_info.py 还需要 CORE_DB_TYPE
CORE_DB_TYPE = ["MSG", "MicroMsg", "MediaMSG", "FTSMSG", "Sns", "Emotion", "Favorite"]
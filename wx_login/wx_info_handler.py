# wx_login/wx_info_handler.py (最终完整版)

import ctypes
import json
import os
import re
import winreg
from typing import List, Union
import traceback
from core_utils import *
import ctypes.wintypes as wintypes

PROCESS_QUERY_INFORMATION = 0x0400
PROCESS_VM_READ = 0x0010
kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)
OpenProcess = kernel32.OpenProcess
OpenProcess.restype = wintypes.HANDLE
OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
CloseHandle = kernel32.CloseHandle
CloseHandle.restype = wintypes.BOOL
CloseHandle.argtypes = [wintypes.HANDLE]
ReadProcessMemory = kernel32.ReadProcessMemory
void_p = ctypes.c_void_p

@wx_core_error
def get_key_by_offs(h_process, address, address_len=8):
    array = ctypes.create_string_buffer(address_len)
    if ReadProcessMemory(h_process, void_p(address), array, address_len, 0) == 0: return None
    address = int.from_bytes(array, byteorder='little')
    key = ctypes.create_string_buffer(32)
    if ReadProcessMemory(h_process, void_p(address), key, 32, 0) == 0: return None
    return bytes(key).hex()

@wx_core_error
def get_info_string(h_process, address, n_size=64):
    array = ctypes.create_string_buffer(n_size)
    if ReadProcessMemory(h_process, void_p(address), array, n_size, 0) == 0: return None
    array = bytes(array).split(b"\x00")[0] if b"\x00" in array else bytes(array)
    return array.decode('utf-8', errors='ignore').strip()

@wx_core_error
def get_info_name(h_process, address, address_len=8, n_size=64):
    array = ctypes.create_string_buffer(n_size)
    if ReadProcessMemory(h_process, void_p(address), array, n_size, 0) == 0: return None
    address1 = int.from_bytes(array[:address_len], byteorder='little')
    info_name = get_info_string(h_process, address1, n_size)
    if info_name: return info_name
    array = bytes(array).split(b"\x00")[0] if b"\x00" in array else bytes(array)
    return array.decode('utf-8', errors='ignore').strip()

@wx_core_error
def get_info_wxid(pid, h_process):
    memory_maps = get_memory_maps(pid)
    start_addr, end_addr = 0, 0
    for module in memory_maps:
        if hasattr(module, 'path') and 'WeChatWin.dll' in module.path:
            s = int(module.addr, 16) if isinstance(module.addr, str) else int(module.addr)
            e = s + module.rss
            if start_addr == 0: start_addr = s
            end_addr = max(end_addr, e)
    if start_addr == 0: return None
    addrs = search_memory(h_process, br'\\Msg\\FTSContact', max_num=100, start_address=start_addr, end_address=end_addr)
    if not addrs: return None
    wxids = []
    for addr in addrs:
        array = ctypes.create_string_buffer(80)
        if ReadProcessMemory(h_process, void_p(addr - 30), array, 80, 0) == 0: continue
        array = bytes(array).split(b"\\Msg")[0]
        array = array.split(b"\\")[-1]
        wxid = array.decode('utf-8', errors='ignore')
        if wxid.startswith("wxid_"): wxids.append(wxid)
    return max(wxids, key=wxids.count) if wxids else None

@wx_core_error
def get_wx_dir_by_reg(wxid="all"):
    try:
        key = winreg.OpenKey(winreg.HKEY_CURRENT_USER, r"Software\Tencent\WeChat", 0, winreg.KEY_READ)
        value, _ = winreg.QueryValueEx(key, "FileSavePath")
        winreg.CloseKey(key)
        w_dir = value
    except Exception:
        profile = os.environ.get("USERPROFILE")
        w_dir = os.path.join(profile, "Documents")
    wx_dir = os.path.join(w_dir, "WeChat Files")
    if wxid != "all":
        wxid_dir = os.path.join(wx_dir, wxid)
        return wxid_dir if os.path.exists(wxid_dir) else None
    return wx_dir if os.path.exists(wx_dir) else None

def get_wx_dir(wxid):
    return get_wx_dir_by_reg(wxid)

def get_info_details(pid, WX_OFFS: dict = None, wechat_files_path: str = None):
    try:
        path = get_process_exe_path(pid)
        if not path: return {}
        version = get_file_version_info(path)
        rd = {'pid': pid, 'version': version}
        bias_list = WX_OFFS.get(rd['version'])
        Handle = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid)
        if not Handle: return rd

        addrLen = get_exe_bit(path)
        wechat_base_address = 0
        if bias_list:
            memory_maps = get_memory_maps(pid)
            for module in memory_maps:
                if hasattr(module, 'path') and 'WeChatWin.dll' in module.path:
                    wechat_base_address = int(module.addr, 16) if isinstance(module.addr, str) else int(module.addr)
                    break
        if not wechat_base_address:
            CloseHandle(Handle)
            return rd
        
        # 黄金路径：一次性获取所有信息
        key_addr = wechat_base_address + bias_list[4]
        rd['key'] = get_key_by_offs(Handle, key_addr, addrLen)
        
        # 黄金路径的第二部分：用 Key 匹配 wxid
        if not wechat_files_path or not os.path.exists(wechat_files_path):
             CloseHandle(Handle)
             return rd
        all_wx_users = [d for d in os.listdir(wechat_files_path) if os.path.isdir(os.path.join(wechat_files_path, d)) and d.startswith('wxid_')]
        for user_wxid in all_wx_users:
            user_dir = os.path.join(wechat_files_path, user_wxid)
            db_path_to_verify = os.path.join(user_dir, "Msg", "MicroMsg.db")
            if os.path.exists(db_path_to_verify) and verify_key(rd.get('key'), db_path_to_verify):
                rd['wxid'] = user_wxid
                rd['wx_dir'] = user_dir
                # 获取其他信息作为补充
                name_addr = wechat_base_address + bias_list[0]
                account_addr = wechat_base_address + bias_list[1]
                mobile_addr = wechat_base_address + bias_list[2]
                rd['nickname'] = get_info_name(Handle, name_addr, addrLen)
                rd['account'] = get_info_string(Handle, account_addr)
                rd['mobile'] = get_info_string(Handle, mobile_addr)
                break
        
        CloseHandle(Handle)
        return rd
    except Exception:
        traceback.print_exc()
        return {}

def get_wx_info(WX_OFFS: dict = None, wechat_files_path: str = None):
    result = []
    wechat_pids = [pid for pid, name in get_process_list() if name == "WeChat.exe"]
    if not wechat_pids:
        print("[错误] 未找到正在运行的 WeChat.exe 进程。")
        return []
    
    for pid in wechat_pids:
        user_info = get_info_details(pid, WX_OFFS, wechat_files_path)
        if user_info:
            result.append(user_info)
    return result
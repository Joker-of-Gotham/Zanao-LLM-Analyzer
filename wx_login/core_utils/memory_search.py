# wx_login/core_utils/memory_search.py
import ctypes
import ctypes.wintypes as wintypes
from ._loger import wx_core_error

ReadProcessMemory = ctypes.windll.kernel32.ReadProcessMemory
void_p = ctypes.c_void_p

@wx_core_error
def search_memory(h_process, search_bytes, max_num=0, start_address=0, end_address=0x7FFFFFFFFFFFFFFF):
    found_addresses = []
    chunk_size = 4096
    buffer = ctypes.create_string_buffer(chunk_size)
    current_address = start_address

    while current_address < end_address:
        bytes_read = ctypes.c_size_t(0)
        try:
            if ReadProcessMemory(h_process, void_p(current_address), buffer, chunk_size, ctypes.byref(bytes_read)) == 0:
                current_address += chunk_size
                continue
        except (BufferError, ValueError, TypeError):
            current_address += chunk_size
            continue

        if bytes_read.value > 0:
            chunk = buffer.raw[:bytes_read.value]
            offset = 0
            while True:
                pos = chunk.find(search_bytes, offset)
                if pos == -1:
                    break
                found_addr = current_address + pos
                found_addresses.append(found_addr)
                if max_num != 0 and len(found_addresses) >= max_num:
                    return found_addresses
                offset = pos + 1
        current_address += chunk_size
    return found_addresses
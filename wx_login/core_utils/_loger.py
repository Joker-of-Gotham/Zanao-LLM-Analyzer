# wx_login/core_utils/_loger.py

import logging
import traceback

# 创建一个日志记录器
wx_core_loger = logging.getLogger('wx_core')
wx_core_loger.setLevel(logging.INFO)

# 创建一个控制台处理器 (如果需要显示日志，可以取消注释)
# console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# console_handler.setFormatter(formatter)
# wx_core_loger.addHandler(console_handler)

def wx_core_error(func):
    """
    一个简单的错误处理装饰器，用于捕获和记录异常。
    """
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wx_core_loger.error(f"Function '{func.__name__}' raised an exception: {e}")
            # traceback.print_exc() # 在调试时可以取消注释
            return None
    return wrapper
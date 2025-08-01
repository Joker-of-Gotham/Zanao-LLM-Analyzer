# zanao_climber/utils.py
import time
import random
import hashlib
from zanao_climber import config

def get_nd(length=20):
    """Generates a random string of digits."""
    return ''.join(str(random.randint(0, 9)) for _ in range(length))

def md5_hash(s: str) -> str:
    """Computes the MD5 hash of a string."""
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def get_headers(user_token: str, school_alias: str) -> dict:
    """
    生成与真实客户端完全一致的请求头。
    """
    nd = get_nd()
    td = int(time.time())
    salt = config.API_SALT
    sign_str = f"{school_alias}_{nd}_{td}_{salt}"
    ah = md5_hash(sign_str)
    
    # --- 核心修复：使用您新抓取的Headers作为模板 ---
    return {
        "Host": "api.x.zanao.com",
        "Connection": "keep-alive",
        # "Content-Length" 由requests库自动计算，我们不需要手动设置
        "X-Sc-Version": "your_version",                                         # 这个填你的版本号
        "X-Sc-Nwt": "", # 真实请求中为空，我们保持为空
        "X-Sc-Wf": "",  # 新增Header
        "X-Sc-Cloud": "0", # 新增Header
        "X-Sc-Platform": "your_platform",                                       # 这个填你的平台
        "X-Sc-Appid": "your_Appid",                                             # 这里填你的appid
        "Content-Type": "application/x-www-form-urlencoded", # 保持不变
        "xweb_xhr": "1",
        # 使用最新的User-Agent
        "User-Agent": "your_version",                                           # 这里填你的User-Agent
        "Accept": "*/*",
        # 新增的Sec-Fetch-*系列Headers，最好都加上
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://servicewechat.com/wx3921ddb0258ff14f/82/page-frame.html",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",

        # 以下是动态生成的，保持不变
        "X-Sc-Alias": school_alias, 
        "X-Sc-Od": user_token, 
        "X-Sc-Nd": nd,
        "X-Sc-Td": str(td), 
        "X-Sc-Ah": ah,
    }
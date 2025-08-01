# debug_mx_threadlist.py

import requests
import time
import random
import hashlib
import json
from pprint import pprint

# =============================================================
#  配置区域 (请确保这里的Token是最新的)
# =============================================================
CONFIG = {
    "USER_TOKEN": "azlDNnRNbDZmYnU5dDUzWmZHVEFuNWlMMHFtTTI2ZlpqSWw5MmIySmg2bUQwTEs2c2FLanJyV29nNnlBZUxpRmoyZXNzNWVubTlhYW42N1h1b2xxM0lDVXlMMjdqSGlUeGM2SDNwV0h0SFdKcEp1YmpMZUZ0bytlaU5ySG5XcXloYzIydXNtTGphV3l1SEdYZ0hxOG80cHB5WitEczFpV2VWZHFwUT09",
    "SCHOOL_ALIAS": "sysu",
    "API_SALT": "1b6d2514354bc407afdd935f45521a8c",
    "MX_TAG_HOT_URL": "https://api.x.zanao.com/mx/tag/hot",
    "MX_TAG_THREADLIST_URL": "https://api.x.zanao.com/mx/tag/threadlist"
}

# =============================================================
#  核心辅助函数 (与我们主项目中的版本一致)
# =============================================================
def get_nd(length=20):
    return ''.join(str(random.randint(0, 9)) for _ in range(length))

def md5_hash(s: str) -> str:
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def get_headers(user_token: str, school_alias: str) -> dict:
    nd, td, salt = get_nd(), int(time.time()), CONFIG["API_SALT"]
    sign_str = f"{school_alias}_{nd}_{td}_{salt}"
    ah = md5_hash(sign_str)
    
    return {
        "Host": "api.x.zanao.com",
        "Connection": "keep-alive",
        "X-Sc-Version": "3.4.8",
        "X-Sc-Nwt": "",
        "X-Sc-Wf": "",
        "X-Sc-Cloud": "0",
        "X-Sc-Platform": "windows",
        "X-Sc-Appid": "wx3921ddb0258ff14f",
        "Content-Type": "application/x-www-form-urlencoded",
        "xweb_xhr": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 MicroMessenger/7.0.20.1781(0x6700143B) NetType/WIFI MiniProgramEnv/Windows WindowsWechat/WMPF WindowsWechat(0x63090c37)XWEB/14185",
        "Accept": "*/*",
        "Sec-Fetch-Site": "cross-site",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://servicewechat.com/wx3921ddb0258ff14f/82/page-frame.html",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "X-Sc-Alias": school_alias, 
        "X-Sc-Od": user_token, 
        "X-Sc-Nd": nd,
        "X-Sc-Td": str(td), 
        "X-Sc-Ah": ah,
    }

def make_api_request(url, data):
    """一个简单的请求函数，带详细的诊断日志"""
    print(f"\n{'='*20} 准备发起请求 {'='*20}")
    
    headers = get_headers(CONFIG["USER_TOKEN"], CONFIG["SCHOOL_ALIAS"])
    
    print(f"请求URL: {url}")
    print("请求头 (Headers):")
    pprint(headers)
    print("请求体 (Body/Data):")
    pprint(data)
    
    try:
        response = requests.post(url, headers=headers, data=data, verify=False, timeout=20)
        print(f"\n--- 服务器响应 ---")
        print(f"状态码 (Status Code): {response.status_code}")
        print("响应头 (Response Headers):")
        pprint(dict(response.headers))
        
        # 尝试解析JSON并打印
        try:
            json_response = response.json()
            print("响应体 (JSON Body):")
            # 使用json.dumps确保中文正常显示
            print(json.dumps(json_response, indent=2, ensure_ascii=False))
            return json_response
        except json.JSONDecodeError:
            print("响应体 (Raw Text):")
            print(response.text)
            return None

    except requests.exceptions.RequestException as e:
        print(f"\n!!!!!! 请求发生严重错误 !!!!!!")
        print(e)
        return None
    finally:
        print(f"{'='*20} 请求结束 {'='*20}\n")


# =============================================================
#  主诊断逻辑
# =============================================================
def main():
    print("--- 步骤1: 获取热门话题列表 ---")
    hot_tags_json = make_api_request(CONFIG["MX_TAG_HOT_URL"], data={})

    if not (hot_tags_json and hot_tags_json.get('errno') == 0 and 'data' in hot_tags_json and 'list' in hot_tags_json['data']):
        print("!!!!!! 无法获取热门话题列表，诊断中止。 !!!!!!")
        return
        
    hot_tags_list = hot_tags_json['data']['list']
    if not hot_tags_list:
        print("!!!!!! 热门话题列表为空，诊断中止。 !!!!!!")
        return
        
    # 随机选择一个话题进行测试
    tag_to_test = random.choice(hot_tags_list)
    tag_id = tag_to_test.get('tag_id')
    tag_name = tag_to_test.get('name')

    if not tag_id:
        print(f"!!!!!! 随机选择的话题 '{tag_name}' 没有 'tag_id'，诊断中止。 !!!!!!")
        return

    print(f"\n--- 步骤2: 尝试获取话题 '{tag_name}' (ID: {tag_id}) 的帖子列表 ---")
    
    # 构造请求 /mx/tag/threadlist 的参数
    threadlist_data = {
        'tag_id': tag_id,
        'from_time': int(time.time()) # 从当前时间开始获取
    }
    
    make_api_request(CONFIG["MX_TAG_THREADLIST_URL"], data=threadlist_data)

if __name__ == '__main__':
    main()
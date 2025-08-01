# zanao_climber/config.py

# --- User and API Configuration ---
# 推荐使用多个Token，程序会随机选用，降低单个Token被封风险
USER_TOKENS = [
    # "在这里可以添加您的第一个Token",
    # "在这里可以添加您的第二个Token",
    # "在这里可以添加您的第三个Token",
]
SCHOOL_ALIAS = "your_school_name"   # 这里也可以利用抓包软件抓包
API_SALT = "your_api_salt"           # 这个可以通过对小程序抓包或逆向得到

# --- Crawler Behavior Configuration ---
MAX_PAGES_TO_FETCH = 500  # 单次扫描最多翻页数
# (生产者) 翻页基础延时(秒)
PRODUCER_BASE_DELAY = 2.0
# (生产者) 翻页随机额外延时(秒)，最终延时 = BASE + random(0, RANDOM)
PRODUCER_RANDOM_DELAY = 7.5

# (工人) 处理任务的基础延时(秒)
WORKER_BASE_DELAY = 4.0
# (工人) 处理任务的随机额外延时(秒)
WORKER_RANDOM_DELAY = 8.0
# (工人) 单个任务失败后的最大重试次数
MAX_TASK_RETRIES = 3
# (工人) 并发线程数
CONCURRENT_WORKERS = 5 # 这是一个比较均衡的值

# (生产者) 增量扫描的间隔时间(秒)
INCREMENTAL_SCAN_INTERVAL = 1800 # 30分钟

# --- API Endpoints (已根据抓包数据全面修正) ---
BASE_URL = "https://api.x.zanao.com"
THREAD_LIST_URL = f"{BASE_URL}/thread/v2/list"
THREAD_INFO_URL = f"{BASE_URL}/thread/info"
COMMENT_LIST_URL = f"{BASE_URL}/comment/list"
MX_TAG_HOT_URL = f"{BASE_URL}/mx/tag/hot"
MX_TAG_THREADLIST_URL = f"{BASE_URL}/mx/tag/threadlist"
MX_THREAD_INFO_URL = f"{BASE_URL}/mx/thread/info"
MX_COMMENT_LIST_URL = f"{BASE_URL}/mx/comment/list"

# --- Redis和数据库配置 ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_QUEUE_NAME = 'zanao_task_queue'
# 用于生产者和消费者通信的Redis键
REDIS_CONTROL_SIGNAL_KEY = 'zanao:control:signal' # PAUSE, CONTINUE, STOP
REDIS_BATCH_TOTAL_KEY = 'zanao:batch:total'
REDIS_DONE_CHANNEL = 'zanao:channel:done'

# 数据库文件名
DB_POSTS_FILENAME = "inschool_posts_and_comments.db"
DB_MX_FILENAME = "outschool_mx_tags_data.db"
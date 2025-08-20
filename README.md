# Zanao-LLM-Analyzer

# **赞哦校园集市分析助手**

中文：
在这个信息日益庞杂的年代，我们不得不被迫查看过量的信息，陷入到信息漩涡中。带来的后果就是，效率下降、内心焦虑、静不下心等不良影响。因此，我决定创建一个赞哦校园集市信息分析助手，用于信息提取分类，这样既可以快速追踪热帖，也不会漏掉关键信息，更能让自身感受到信息掌控力从而潜心提升自我。

提取内容：
帖子的具体内容
帖子的评论信息
校内热帖
跨校热帖
跨校评论
（持续更新完善）

**我的博客网址**：https://joker-of-gotham.github.io/

博客内针对该项目具有更详细的阐述，在此便不再赘述

# 项目实现

## python编程环境的创建

这里推荐使用`Anaconda prompt`进行环境创建，命令如下：

```bash
conda create -n "Zanao-Climber" python==3.11
conda activate Zanao-Climber
python -m pip install --upgrade pip
pip install -r requirements.txt
```

如果环境配置出现问题，可能是因为有些包支持的安装方式不同；遇到这种问题时，可以自行搜索资料进行过解决。

## 微信群爬虫的实现

### 前置条件

首先，请确定你的微信版本号在`WX_OFFS.json`中存在；如下述`json`中"3.9.12.55"即为版本号

```json
  "3.9.12.55": [
    94550988,
    94552544,
    94551016,
    0,
    94552480
  ]
```

### 运行步骤

使用管理员权限启动`Anaconda Prompt`，使用 `cd` 命令进入相应目录，然后激活您创建的`Conda`环境，然后执行主程序：

```bash
# 替换 "D:\path\to\your\project" 为您的实际项目路径
cd "D:\path\to\your\project"
# 激活环境
conda activate Zanao-Climber
# 执行主程序
python wx_login/main.py
```

### 交互指引

当您运行主程序后，它将引导您完成后续操作。通常会包括以下几个阶段：

- **自动获取微信信息**：程序会首先尝试自动从内存中获取已登录微信的密钥等信息；如果成功，您会看到类似 **“最终成功获取到 X 个有效微信用户的信息！”** 的提示，并显示部分用户信息
- **选择群聊**：程序会自动解密并读取您的群聊列表，然后将其展示在终端中，每个群聊前面都有一个编号；您需要根据提示，输入您想导出聊天记录的群聊所对应的编号，然后按回车
    ```plaintext
    --- 可用群聊列表 ---
    [1] 技术交流与学习群
    [2] 周末羽毛球小分队
    [3] 公司项目A通知群
    ```
- **输入日期范围**：接下来，程序会要求您指定需要导出的聊天记录的时间范围；请按照 YYYY-MM-DD 的格式依次输入开始日期和结束日期，每次输入后按回车
    ```plaintext
    开始日期 YYYY-MM-DD: 2024-01-01
    结束日期 YYYY-MM-DD: 2024-07-31
    ```
- **自动处理与导出**：完成以上输入后，程序会自动查询、整理并导出数据；您会看到一系列处理进度的日志，如“正在查询聊天记录...”、“正在导出消息到...”；当看到“导出完成！”和“所有流程已完成”的提示后，表示任务已成功；导出的聊天记录文件（`.txt` 或 `.csv` 格式）会保存在项目根目录下的 `data/original_data/` 文件夹中
- **格式整理**：在完成导出后，您可以运行文件夹`format_polisher`下的`integrated_cleaner.py`程序进行格式修正

除了交互式操作，您也可以在运行命令时直接通过参数指定所有选项，以实现自动化，如：

```bash
# 导出第3个群聊，从2024-05-01到2024-05-31的记录，并保存为csv格式
python wx_login/main.py --chat-index 3 --start 2024-05-01 --end 2024-05-31 --format csv
```

- 可用参数说明:
    - `--chat-index <编号>`: 直接指定群聊编号。
    - `--start <日期>`: 指定开始日期 (格式: `YYYY-MM-DD`)。
    - `--end <日期>`: 指定结束日期 (格式: `YYYY-MM-DD`)。
    - `--format <格式>`: 指定导出格式，可选值为 `txt` 或 `csv` (默认为 `txt`)。
    - `--wechat-path <路径>`: 如果您的 "WeChat Files" 文件夹不在默认位置，可以使用此参数指定其完整路径。

## 集市爬虫的实现

### 前置条件

在实现本部分前需要配确认以下条件：

- **Redis 服务**：请确保您已在本地或服务器上安装并启动了`Redis`服务，对于 `Windows` 用户，可以从 `Redis on Windows` 下载并安装；默认配置下，程序会连接到 `localhost` 的 `6379` 端口。如果您 `Redis` 的地址或端口不同，请修改 `zanao-climber/config.py` 文件中的 `REDIS_HOST` 和 `REDIS_PORT`
- **获取 User Token**：您需要通过网络抓包工具（如 `Fiddler`, `Charles`, `mitmproxy` 等）拦截微信小程序“在学校”的网络请求，从中找到 `X-Sc-Od` 请求头，其对应的值就是您的 `User Token`；强烈建议使用多个不同的微信账号获取多个 `User Token`，因为程序会随机选用，这可以有效分摊请求压力，降低单个账号被限制的风险
- 配置 `config.py`：打开 `zanao-climber/config.py` 文件，将您获取到的一个或多个 `User Token` 填入 `USER_TOKENS` 列表中；根据您的需求，可以调整 `CONCURRENT_WORKERS`（并发线程数）和 `*_DELAY`（请求延时）等参数，**过高的并发和过低的延时会显著增加账号被临时封禁的风险**；注意用你自己的相关信息填写`config.py`和`utils.py`中标注出的应填部分

### 交互指引

本系统采用生产者 (`main.py`) 和消费者 (`worker.py`)分离的设计，它们是两个需要同时运行的独立进程。您需要至少打开两个终端来分别启动它们。

#### 容器的创建、启动与停止

首先，您需要在Docker中启动Redis服务，具体方法为打开一个终端，运行以下命令来拉取最新的Redis镜像并启动一个名为 `zanao-redis` 的容器，相关操作如下：

```bash
# 创建新容器，并设置为只要Docker打开就自动启动该容器
docker run -d --name zanao-redis -p 6379:6379 --restart always redis
# 检查该容器是否在运行，输出列表中包含 zanao-redis，且状态为 Up，则表示Redis服务成功启动
docker ps
# 在关闭该容器后重启该容器
docker start zanao-redis
# 停止该容器
docker stop zanao-redis
# 重启该容器
docker restart zanao-redis
```

**命令分解说明**:

- `docker run`: 运行一个新容器。
- `-d`: 后台运行 (Detached mode)，容器会在后台持续运行。
- `--name zanao-redis`: 为容器指定一个友好的名称，方便后续管理。
- `-p 6379:6379`: 端口映射。将您电脑（主机）的`6379`端口映射到容器内部的`6379`端口。这是让您的Python代码能连接到容器内Redis的关键。
- `--restart always`: 自动重启。设置容器总是在Docker服务启动或容器意外退出时自动重启，确保服务的持续可用性。
- `redis`: 要使用的Docker镜像名称。Docker会自动从Docker Hub上拉取官方的Redis镜像。

#### 启动爬虫进程

现在，Redis服务已在后台运行，配置也已完成。您可以启动爬虫的两个核心进程了。您需要至少打开两个终端。

打开第一个终端 (保持第一个终端的消费者仍在运行)，进入项目根目录并激活Conda环境，运行 `main.py` 脚本来启动生产者进程。

```bash
cd "D:\path\to\your\project"
conda activate Zanao-Climber
python zanao-climber/main.py
```

打开第二个终端，进入项目根目录，并激活您的Conda环境，运行 `worker.py` 脚本来启动消费者进程。

```bash
cd "D:\path\to\your\project"
conda activate Zanao-Climber
python zanao-climber/worker.py
```

在生产者终端选择任务后，您可以在两个终端分别观察到任务的分发进度和处理进度；在生产者终端 (`main.py`) 中，按 `Ctrl+C` 或根据提示输入 `q` 退出，生产者退出时会自动通知消费者停止工作，消费者终端 (`worker.py`) 会在处理完当前所有任务后自动、安全地退出。

## 集市数据分析与信息采集的实现

### 前置条件

在实现本部分前需要配确认以下条件：

- 硬件要求：本系统的AI模型（特别是NER和Embedding模型）计算量较大，强烈推荐在配备有 NVIDIA GPU 和足够显存的机器上运行，以获得理想的处理速度；如果只有CPU，程序仍可运行，但AI分析（尤其是实时流水线）的速度会非常缓慢
- 依赖服务：`Ollama` 服务，确保您本地的 `Ollama` 服务正在后台运行。本系统通过API调用Ollama来执行部分AI推理任务；确保“集市爬虫”已经运行过，并且在 `data/zanao_detailed_info/` 目录下已经生成了 `inschool_posts_and_comments.db` 和 `outschool_mx_tags_data.db` 这两个包含原始数据的数据库文件
- 模型文件：首次运行本模块的脚本时，程序会自动从Hugging Face等模型社区下载所需的AI模型到您的本地缓存中（通常在用户目录的 `.cache/huggingface` 下）。这个过程可能需要较长时间并占用一定的磁盘空间，请确保网络连接通畅。

### 首次运行：初始化与数据库准备

在第一次运行本分析系统时，您必须打开一个终端，使用`cd`进入项目根目录，并激活您的Conda环境，初始化原数据库和创建新数据库，看到 `All tables for analysis.db have been set up successfully...` 则表示成功。

```bash
# 准备原始数据库，为原始数据库添加 analysis_status 字段，用于后续的增量分析
python zanao_analyzer/source_db_preparer.py
# 创建分析结果数据库，即 analysis.db 文件，并设置好所有用于存储分析结果的表结构
python zanao_analyzer/database_setup.py
```

### 核心功能运行

初始化完成后，您可以根据需要运行系统的核心功能。通常分为实时处理、批量处理和API服务三个部分，您可以根据需求选择启动；当有进程未退出时，你可以选择用`Ctrl+C`终止该程序，或者重新开一个终端进行其它部分的运行。

```bash
# 持续地将原始数据进行基础AI分析并存入分析库
python zanao_analyzer/execution/run_realtime_pipeline.py
# 全局统计和深度分析
python zanao_analyzer/execution/run_batch_analytics.py
```

如果您需要通过Dify平台或其他应用来调用本系统的分析能力，请启动API服务器。服务启动后，在您的浏览器中访问 `http://127.0.0.1:5060/docs`，您会看到一个可交互的 Swagger UI 界面，其中详细列出了所有可用的API端点、参数和响应格式。您可以直接在这个页面上对API进行测试。

```bash
# 启动API服务器
uvicorn zanao_analyzer.api_server:app --host 0.0.0.0 --port 5060
```

### 查看与结果分析

API服务提供了 `/tools/generate_chart` 端点，调用后生成的图表会保存在 `zanao_analyzer/generated_charts/` 目录下，这些 `.html` 文件可以直接用浏览器打开查看。

如果您想清空所有分析结果，并从头开始重新分析，可以运行清理脚本，删除所有分析数据。程序会要求您输入 `y` 进行二次确认。确认后，`analysis.db` 中的数据将被清空，同时原始数据库中的 `analysis_status` 标志位也会被重置为0。

```bash
python zanao_analyzer/data_cleanup.py
```

## Dify工作流实现

### Dify的准备与启动

你可以利用Git工具将Dify克隆到你的项目目录下，然后按照以下l流程创建Dify的相关容器：

- 在创建前请务必在`dify\docker\volumes\sandbox\dependencies\python-requirements.txt`中加入：

    ```plaintext
    keybert
    sentence-transformers
    cachetools
    ```

- 同时修改`dify\docker\docker-compose.yaml`：

    ```yaml
    services:
    # ... 其他服务 ...
    sandbox:
        # ... 其他配置 ...
        volumes:
        # ... 其他挂载 ...
        - '/mnt/<你存储该项目的硬盘>/<你存储该项目的位置>/Zanao-LLM-Analyzer/data/zanao_detailed_info:/data:ro'
    ```
- 然后启动Dify
    ```bash
    cd dify
    cd docker
    cp .env.example .env
    docker compose up -d
    ```
- 最后复制该网址，启动浏览器访问Dify界面：`http://localhost/install`

### Dify中模型的导入

首先下载ollama客户端，然后启动`CMD`或`Windows PowerShell`拉取模型，这里本人拉取的有两个模型，一个作为文本嵌入模型，一个作为语言模型：

```bash
ollama pull granite-embedding:278m
ollama pull gemma3
```

如果在后续运行过程中模型启动出现问题，你可以最小化于任务栏的ollama，转而在`CMD`或`Windows PowerShell`中启动：

```bash
ollama serve
```

然后点击右上角用户头像进入设置，点击左侧栏“工作空间”中的“模型供应商”，在“安装模型供应商”部分中找到ollama并安装，然后在上方的模型列表中从Ollma添加模型；模型名称填写你已经下载了的模型名称，基础URL填写Ollama Server的基础URL，其它部分按照模型特性填写即可。

### Dify中自定义工具的导入

进入 `““工具” -> “创建自定义工具”`，工具类型选择“基于OpenAPI规范导入”，在Schema地址中填入您 `Zanao Analyzer API` 服务的 `openapi.json` 地址(一般为`http://<您的IP地址>:5060/openapi.json`)，然后按照步骤完成工具的设置，即可得到可以在工作流中插入的工具。

### Dify中工作流的创建

进入 `“工作室” -> “全部” -> “创建空白应用”`，选择`Chatflow`创建支持记忆的复杂多轮对话工作流，然后利用自定义工具和ollma上拉取到的模型，基于Dify提供的节点模块进行个性化的工作流设计，从而实现集市信息的提取、分类、舆情分析和资料抽取。

# 结果呈现

## 工作流结果呈现

本人将工作流设计为两条，一条负责进行舆情分析，另一条负责具体信息查询与引用，整体工作流设计如下图。

<div style="text-align: center;">
<img src="/doc_image/工作流.png" alt="描述文字" width="880" height="380">
</div>

## 交互实现

### 分支一：细节问答

案例输出结果如下图：
    <div style="text-align: center;">
<img src="/doc_image/分支一.png" alt="描述文字" width="780" height="490">
</div>

### 分支二：舆情分析

案例输出结果部分如下：
    <div style="text-align: center;">
<img src="/doc_image/分支二.png" alt="描述文字" width="780" height="530">
</div>

### 分支三：资源检索

案例结果部分输出如下：
    <div style="text-align: center;">
<img src="/doc_image/资源检索.png" alt="描述文字" width="780" height="490">
</div>


## 统计图呈现

得到的统计图包括：热帖分析条形图、情绪分布饼图、情绪时序图、全局词云图和用户词云图。为节省空间，现组合呈现如下：
<div style="text-align: center;">
<img src="/doc_image/统计图呈现.png" alt="描述文字" width="880" height="800">
</div>

## 数据库呈现

得到的数据库包括：`inschool_posts_and_comments.db`、`outschool_mx_tags_data.db`、`analysis.db`。现以`inschool_posts_and_comments.db`中的帖子部分为例，部分呈现如下：

<div style="text-align: center;">
<img src="/doc_image/数据库.png" alt="描述文字" width="880" height="450">
</div>

# 后续更新方向

- [ ] 优化爬虫设计，提升爬虫的执行效率和稳定性，提高爬虫安全性和单次`User Token`使用时间，提升爬虫程序的智能型和自主性
- [ ] 优化数据库设计，针对不同数据库的不同表目，进行整合和调整，提高信息密度和丰富度；同时增加多模态数据库，存储如图片等非文本信息
- [ ] 优化代码流程设计，提供更加高效稳定的并行化处理方案，优化文件结构，优化API接口设计，提高处理效率，提供稳定的多平台调用方式
- [x] 着手UI设计，开始建立前端页面，设计更加现代、好用、一体化的UI界面，提供更好的交互体验：现UI已上传至相应的[Github链接](https://github.com/Joker-of-Gotham/Zanao-LLM-Analyzer-frontend)
- [ ] 优化工作流设计，提升硬件水平，提高模型能力，采用更先进的文本处理方案，引入多模态数据，优化工作流节点和字段设计，提高单次对话的处理效率和多轮对话连贯性，加强用户体验
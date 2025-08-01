import re
import csv
import os

# --- 配置目录路径 ---
# 获取当前脚本的绝对路径
script_dir = os.path.dirname(os.path.abspath(__file__))
# 向上移动一个目录层级，找到 '完整流程实现' 目录
base_dir = os.path.dirname(script_dir)

ORIGINAL_DATA_DIR = os.path.join(base_dir, 'data', 'original_data')
POLISHED_DATA_DIR = os.path.join(base_dir, 'data', 'polished_data')

def _extract_title_and_url(content: str) -> list:
    """
    一个内部辅助函数，用于从消息内容中提取标题和 URL 对。
    """
    # 修复后的正则表达式，只包含两个捕获组：一个用于标题，一个用于完整的URL。
    # 使用(?:mp|https)来使协议部分不被捕获，确保findall只返回标题和完整URL。
    title_url_pattern = re.compile(r'\s*([^\n\r].*?)\n\s*((?:mp|https)://[^\n\r]+)', re.DOTALL)
    title_url_pairs = title_url_pattern.findall(content)
    
    # 格式化并返回结果
    result = []
    for title, url in title_url_pairs:
        result.append({'title': title.strip(), 'url': url.strip()})
    return result

def _clean_txt_content(raw_text: str) -> list:
    """
    清洗原始导出的 .txt 文件的内容。
    """
    cleaned_data = []
    # 正则表达式匹配完整的消息块，包括时间戳和内容
    message_blocks_pattern = re.compile(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\].*?:\s*(.*?)(?=\n\[\d{4}-\d{2}-\d{2}|\Z)', re.DOTALL)
    message_blocks = message_blocks_pattern.findall(raw_text)

    for block_time, block_content in message_blocks:
        extracted_pairs = _extract_title_and_url(block_content)
        for pair in extracted_pairs:
            cleaned_data.append({
                'time': block_time.strip(),
                'title': pair['title'],
                'url': pair['url']
            })
            
    return cleaned_data

def _clean_csv_content(rows) -> list:
    """
    清洗原始导出的 .csv 文件的内容。
    """
    cleaned_data = []
    for row in rows:
        readable_time = row['ReadableTime']
        content = row['Content']
        
        extracted_pairs = _extract_title_and_url(content)
        for pair in extracted_pairs:
            cleaned_data.append({
                'time': readable_time,
                'title': pair['title'],
                'url': pair['url']
            })
            
    return cleaned_data

def process_file(file_path: str) -> list:
    """
    根据文件扩展名自动选择清洗方法。
    """
    file_extension = os.path.splitext(file_path)[1].lower()
    
    cleaned_data = []
    try:
        if file_extension == '.txt':
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_text = f.read()
            cleaned_data = _clean_txt_content(raw_text)
        elif file_extension == '.csv':
            with open(file_path, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                cleaned_data = _clean_csv_content(reader)
        else:
            print(f"警告: 跳过不支持的文件类型 '{file_path}'。")
    except FileNotFoundError:
        print(f"错误: 文件 '{file_path}' 未找到。")
    except Exception as e:
        print(f"处理文件 '{file_path}' 时发生错误: {e}")
        
    return cleaned_data

def save_to_csv(data: list, output_file_path: str):
    """将清洗后的数据保存为CSV文件。"""
    if not data:
        print("没有可保存的数据。")
        return
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    with open(output_file_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = ['time', 'title', 'url']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"数据已成功保存到 '{output_file_path}'。")

if __name__ == '__main__':
    print(f"正在扫描目录: {ORIGINAL_DATA_DIR}")
    
    # 确保原始数据目录存在
    if not os.path.exists(ORIGINAL_DATA_DIR):
        print(f"错误: 原始数据目录 '{ORIGINAL_DATA_DIR}' 不存在。请确认路径是否正确。")
    else:
        # 遍历原始数据目录下的所有文件
        for filename in os.listdir(ORIGINAL_DATA_DIR):
            if filename.endswith(('.txt', '.csv')):
                input_file_path = os.path.join(ORIGINAL_DATA_DIR, filename)
                output_filename = f"cleaned_{os.path.splitext(filename)[0]}.csv"
                output_file_path = os.path.join(POLISHED_DATA_DIR, output_filename)
                
                # 验证是否已清洗过
                if os.path.exists(output_file_path):
                    print(f"文件 '{filename}' 已经清洗过，跳过。")
                else:
                    print(f"开始清洗文件: {filename}")
                    cleaned_data = process_file(input_file_path)
                    
                    if cleaned_data:
                        save_to_csv(cleaned_data, output_file_path)
                    print("-" * 20)
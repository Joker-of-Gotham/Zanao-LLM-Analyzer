# 包含 SentimentAnalyzer 类，负责加载模型并提供一个 analyze(text) 方法，返回标准化的情感结果

# -*- coding: utf-8 -*-
"""
情感分析模块 (SentimentAnalyzer) - 【V4，最终健壮版】
- 动态确定标签索引，彻底解决硬编码带来的问题。
"""
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import config

class SentimentAnalyzer:
    """封装情感分析功能的类"""
    
    def __init__(self):
        """
        初始化模型，并动态确定positive/negative标签对应的索引。
        """
        print("Initializing Sentiment Analyzer...")
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Sentiment Analyzer will use device: {self.device}")

        model_name = config.MODELS.get('sentiment')
        if not model_name:
            raise ValueError("Sentiment model name not found in config.py")

        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(model_name).to(self.device)
        self.model.eval()

        # --- 最终核心修正：动态查找索引 ---
        # 1. 从模型配置中获取 id->label 的映射
        id2label = self.model.config.id2label
        print(f"Sentiment model loaded. Label mapping from model config: {id2label}")
        
        # 2. 遍历映射，找到 'positive' 和 'negative' (不区分大小写) 对应的索引
        self.positive_index = None
        self.negative_index = None
        for i, label in id2label.items():
            if label.lower() == 'positive':
                self.positive_index = i
            elif label.lower() == 'negative':
                self.negative_index = i

        # 3. 校验是否成功找到索引
        if self.positive_index is None or self.negative_index is None:
            raise RuntimeError(
                f"Could not determine positive/negative index from model config: {id2label}. "
                "The model might not be a standard binary sentiment classifier."
            )
        
        print(f"Index mapping confirmed: Negative -> Index {self.negative_index}, Positive -> Index {self.positive_index}")
        # --- 修正结束 ---
        
        print("Sentiment Analyzer initialized successfully.")

    def analyze(self, text: str) -> dict:
        """
        分析单条文本的情感。
        """
        if not text or not isinstance(text, str):
            return {'label': 'neutral', 'score': 0.0}
            
        try:
            with torch.no_grad():
                inputs = self.tokenizer(text, return_tensors="pt", truncation=True, max_length=512).to(self.device)
                outputs = self.model(**inputs)
                
                # 计算概率
                probabilities = torch.nn.functional.softmax(outputs.logits, dim=-1)[0]
                
                # --- 使用动态查找到的索引 ---
                prob_positive = probabilities[self.positive_index].item()
                prob_negative = probabilities[self.negative_index].item()
                # --- 使用结束 ---
                
                # 计算分数和标签
                score = prob_positive - prob_negative
                label = 'positive' if score > 0 else 'negative'
                
                return {'label': label, 'score': round(score, 4)}

        except Exception as e:
            print(f"Error during sentiment analysis for text '{text[:50]}...': {e.__class__.__name__}: {e}")
            return {'label': 'error', 'score': 0.0}
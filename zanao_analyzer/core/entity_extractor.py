# 包含 EntityExtractor 类，负责加载NER模型并提供 extract(text) 方法，返回标准化的实体列表

# -*- coding: utf-8 -*-
"""
命名实体识别模块 (EntityExtractor) - 【V6，带诊断日志的多标签版】
- 恢复多标签一次性调用，以保证性能。
- 在每次调用模型前加入“探针”日志，打印即将使用的标签列表，用于最终诊断。
"""
from gliner import GLiNER
import torch
import config
import time # 引入 time 模块

class EntityExtractor:
    """封装命名实体识别 (NER) 功能的类"""

    def __init__(self):
        """
        初始化GLiNER模型，并从config加载NER标签。
        """
        print("Initializing Entity Extractor...")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Entity Extractor will use device: {self.device}")

        model_name = config.MODELS.get('ner')
        if not model_name:
            raise ValueError("NER model name not found in config.py")

        self.model = GLiNER.from_pretrained(model_name)
        
        # 强制从 config.py 加载专为NER定义的标签列表
        self.default_labels = config.NER_LABELS
        if not self.default_labels or not isinstance(self.default_labels, list):
             raise ValueError("NER_LABELS not correctly defined in config.py")
        
        print(f"Entity Extractor loaded {len(self.default_labels)} default NER labels from config.py: {self.default_labels}")
        print("Entity Extractor initialized successfully.")

    def extract(self, text: str, labels: list = None) -> list:
        """
        从单条文本中提取实体（高效的多标签模式）。

        Args:
            text (str): 需要分析的文本。
            labels (list, optional): 如果提供，则使用此列表覆盖默认标签。

        Returns:
            list: 实体字典的列表, e.g., [{'text': '...', 'label': '...', 'score': ...}]
        """
        target_labels = labels if labels is not None else self.default_labels
        
        if not text or not isinstance(text, str) or not target_labels:
            return []
        
        # ======================= 【诊断探针】 =======================
        # 在调用模型前，打印出当前时间戳和即将使用的标签列表的详细信息。
        # 这是为了捕获“幽灵代码”的铁证。
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        print(f"[{timestamp}] DEBUG_NER: About to call predict_entities.")
        print(f"    - Text (first 30 chars): '{text[:30]}...'")
        print(f"    - Labels to be used ({len(target_labels)} total): {target_labels}")
        # ==========================================================
        
        try:
            # 恢复高效的多标签一次性调用
            entities = self.model.predict_entities(
                text, 
                target_labels, 
                threshold=config.THRESHOLDS.get('ner_threshold', 0.5)
            )
            
            # ======================= 【结果探针】 =======================
            if entities:
                 print(f"    - SUCCESS: Found {len(entities)} entities. Example: {entities[0]}")
            # ===========================================================

            return entities
            
        except Exception as e:
            print(f"    - ERROR: An exception occurred during predict_entities: {e.__class__.__name__}: {e}")
            return []
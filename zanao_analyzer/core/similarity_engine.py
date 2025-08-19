# 包含 SimilarityEngine 类，它会加载 config.py 中定义的分类体系文件。
# 提供 calculate_post_similarity(text1, text2) 方法，调用向量API。
# 提供 match_entities_to_classification(entities) 方法，计算实体列表与内部加载的分类体系的相似度，返回匹配的分类和分数。

# -*- coding: utf-8 -*-
"""
相似度计算模块 (SimilarityEngine) - V2 (支持双重相似度匹配)
- 封装基于 sentence-transformers 的文本向量化和相似度计算。
- 加载分类体系，并提供实体与分类的匹配功能。
- 新增：加载数据库中实际存在的分类，并提供“翻译官”功能，连接查询分类与数据库分类。
"""
import json
import sqlite3
from sentence_transformers import SentenceTransformer, util
import torch
import config

class SimilarityEngine:
    """封装相似度计算和分类匹配功能的类"""

    def __init__(self):
        """
        初始化向量模型和分类体系。
        - 自动使用GPU（如果可用）。
        - 预加载并编码分类体系中的所有标签。
        - 【新增】预加载并编码数据库中所有实际存在的分类标签。
        """
        print("Initializing Similarity Engine...")
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        print(f"Similarity Engine will use device: {self.device}")

        model_name = config.MODELS.get('embedding')
        if not model_name:
            raise ValueError("Embedding model name not found in config.py")

        self.model = SentenceTransformer(model_name, device=self.device)
        
        # --- 步骤 1: 加载并编码 taxonomy.json 中的分类 ---
        self.taxonomy_data = self._load_taxonomy()
        self.classification_labels, self.classification_embeddings = self._precompute_taxonomy_embeddings()
        
        # --- 步骤 2: 【新增】加载并编码数据库中实际存在的分类 ---
        self.db_classification_labels = self._load_db_classifications()
        if self.db_classification_labels:
            self.db_classification_embeddings = self.model.encode(
                self.db_classification_labels, 
                convert_to_tensor=True, 
                show_progress_bar=True,
                device=self.device
            )
            print("Database classifications' embeddings pre-computed.")
        else:
            self.db_classification_embeddings = None
            print("[WARN] No classifications found in the database. The 'translation' feature will be disabled.")
        # --- 新增结束 ---

        print("Similarity Engine initialized successfully.")

    def _load_taxonomy(self) -> dict:
        """从JSON文件加载分类体系"""
        try:
            with open(config.RESOURCE_CLASSIFICATION_FILE_PATH, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"[ERROR] Taxonomy file not found at: {config.RESOURCE_CLASSIFICATION_FILE_PATH}")
            return {}
        except json.JSONDecodeError:
            print(f"[ERROR] Failed to parse taxonomy JSON file.")
            return {}

    def _precompute_taxonomy_embeddings(self):
        """预计算并缓存分类体系中所有二级分类的向量"""
        if not self.taxonomy_data:
            return [], None
        
        print("Pre-computing taxonomy embeddings...")
        all_labels = [item for sublist in self.taxonomy_data.values() for item in sublist]
        
        embeddings = self.model.encode(
            all_labels, 
            convert_to_tensor=True, 
            show_progress_bar=True,
            device=self.device
        )
        print("Taxonomy embeddings pre-computed.")
        return all_labels, embeddings

    def _load_db_classifications(self) -> list:
        """【新增】从 analysis.db 加载所有实际用到的分类标签"""
        print("Loading unique classifications from the database...")
        try:
            # 使用只读模式连接数据库，更安全
            with sqlite3.connect(f"file:{config.ANALYSIS_DB_PATH}?mode=ro", uri=True) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT DISTINCT matched_classification FROM post_classifications")
                rows = cursor.fetchall()
                # 从元组列表中提取字符串
                labels = [row[0] for row in rows if row[0]]
                print(f"[SimilarityEngine] Loaded {len(labels)} unique classifications from the database.")
                return labels
        except Exception as e:
            print(f"[ERROR] Failed to load classifications from the database: {e}")
            return []

    def calculate_post_similarity(self, text1: str, text2: str) -> float:
        """计算两个文本之间的余弦相似度"""
        try:
            embeddings = self.model.encode([text1, text2], convert_to_tensor=True, device=self.device)
            cosine_score = util.cos_sim(embeddings[0], embeddings[1]).item()
            return cosine_score
        except Exception as e:
            print(f"Error calculating similarity: {e}")
            return 0.0

    def match_query_to_classification(self, query_text: str, top_k: int = 3) -> list:
        """
        【第一次匹配】将完整的用户查询文本匹配到 taxonomy.json 中的分类标签。
        """
        if not query_text or self.classification_embeddings is None:
            return []

        try:
            query_embedding = self.model.encode(query_text, convert_to_tensor=True, device=self.device)
            cosine_scores = util.cos_sim(query_embedding, self.classification_embeddings)
            top_results = torch.topk(cosine_scores, k=min(top_k, len(self.classification_labels)), dim=-1)
            
            matches = []
            scores = top_results.values.cpu().flatten().tolist()
            indices = top_results.indices.cpu().flatten().tolist()
            
            for score, idx in zip(scores, indices):
                if score >= config.THRESHOLDS.get('query_classification_match', 0.45):
                    matches.append({
                        'classification': self.classification_labels[idx],
                        'score': round(score, 4)
                    })
            return matches
        except Exception as e:
            print(f"Error matching query to classification: {e}")
            return []

    def get_db_equivalent_classifications(self, query_classifications: list, top_k: int = 3) -> list:
        """
        【第二次匹配 - “翻译官”】
        找到与查询分类最相似的、数据库中实际存在的分类。
        """
        if not query_classifications or self.db_classification_embeddings is None:
            return []

        try:
            query_embeds = self.model.encode(query_classifications, convert_to_tensor=True, device=self.device)
            cosine_scores = util.cos_sim(query_embeds, self.db_classification_embeddings)

            final_db_classes = set()
            for i in range(len(query_classifications)):
                top_results = torch.topk(cosine_scores[i], k=min(top_k, len(self.db_classification_labels)))
                
                # 解包 PyTorch 的 topk 结果
                scores = top_results.values.cpu().tolist()
                indices = top_results.indices.cpu().tolist()

                for score, idx in zip(scores, indices):
                    # 在这里，我们可以用一个比较宽松的阈值来确保能“翻译”成功
                    if score > 0.3: 
                        final_db_classes.add(self.db_classification_labels[idx])
            
            return list(final_db_classes)
        except Exception as e:
            print(f"Error in get_db_equivalent_classifications: {e}")
            return []
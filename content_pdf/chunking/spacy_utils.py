# content_pdf/chunking/spacy_utils.py
from typing import Set, Dict, Any
import logging

logger = logging.getLogger(__name__)


class SpacyTextProcessor:
    """spaCy 텍스트 처리 유틸리티"""
    
    def __init__(self, filter_stop_words: bool = True):
        self.filter_stop_words = filter_stop_words
        self.stop_entities = {"the", "a", "an", "this", "that", "these", "those"}
    
    def extract_entities(self, sent) -> Set[str]:
        """문장에서 엔티티 추출"""
        entities = set()
        
        # Named entities
        for ent in sent.ents:
            if not self._is_stop_entity(ent.text):
                entities.add(ent.text.lower())
        
        # 중요한 명사구도 포함
        for chunk in sent.noun_chunks:
            if len(chunk.text.split()) <= 3 and not self._is_stop_entity(chunk.text):
                entities.add(chunk.text.lower())
        
        return entities
    
    def extract_topics(self, sent) -> Set[str]:
        """문장에서 주제어 추출"""
        topics = set()
        
        for token in sent:
            # 명사, 동사, 형용사 중 stop word가 아닌 것들
            if (token.pos_ in ["NOUN", "PROPN", "VERB", "ADJ"] and 
                not token.is_stop and 
                len(token.text) > 2 and
                token.text.isalpha()):
                topics.add(token.lemma_.lower())
        
        return topics
    
    def _is_stop_entity(self, text: str) -> bool:
        """엔티티가 stop entity인지 확인"""
        return text.lower() in self.stop_entities or len(text) < 2


class SimilarityCalculator:
    """유사도 계산 유틸리티"""
    
    def calculate(
        self,
        current_data: Dict[str, Any],
        new_data: Dict[str, Any],
        current_vector,
        new_vector
    ) -> float:
        """통합 유사도 계산"""
        
        # 1. 엔티티 오버랩 (Jaccard similarity)
        entity_overlap = self._jaccard_similarity(
            current_data.get('entities', set()),
            new_data.get('entities', set())
        )
        
        # 2. 토픽 오버랩 (Jaccard similarity)
        topic_overlap = self._jaccard_similarity(
            current_data.get('topics', set()),
            new_data.get('topics', set())
        )
        
        # 3. 벡터 유사도
        vector_similarity = 0.0
        if current_vector is not None and new_vector is not None:
            try:
                vector_similarity = self._cosine_similarity(current_vector, new_vector)
            except Exception as e:
                logger.debug(f"Vector similarity calculation failed: {e}")
        
        # 가중 평균
        if vector_similarity > 0:
            # 벡터 유사도가 있는 경우: 50% 벡터, 25% 엔티티, 25% 토픽
            similarity = (0.5 * vector_similarity + 
                         0.25 * entity_overlap + 
                         0.25 * topic_overlap)
        else:
            # 벡터가 없는 경우: 50% 엔티티, 50% 토픽
            similarity = (0.5 * entity_overlap + 0.5 * topic_overlap)
        
        return similarity
    
    def _jaccard_similarity(self, set1: Set, set2: Set) -> float:
        """Jaccard 유사도 계산"""
        if not set1 and not set2:
            return 0.0
        
        intersection = len(set1 & set2)
        union = len(set1 | set2)
        
        return intersection / union if union > 0 else 0.0
    
    def _cosine_similarity(self, vec1, vec2) -> float:
        """코사인 유사도 계산"""
        import numpy as np
        
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return np.dot(v1, v2) / (norm1 * norm2)
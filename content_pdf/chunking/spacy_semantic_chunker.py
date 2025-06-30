# content_pdf/chunking/spacy_semantic_chunker.py
import asyncio
from typing import List, AsyncGenerator, Dict, Any, Set
from datetime import datetime, timezone
import uuid
import logging

logger = logging.getLogger(__name__)

# spaCy를 조건부로 import
try:
    import spacy
    SPACY_AVAILABLE = True
except ImportError:
    SPACY_AVAILABLE = False
    logger.warning("spaCy not available. SpacySemanticChunker cannot be used.")

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings

class SpacySemanticChunker(ChunkingStrategy):
    """spaCy 기반 언어학적 의미 청킹 (개선된 버전)"""
    
    def __init__(self):
        if not SPACY_AVAILABLE:
            raise ImportError("spaCy is not installed. Please install it with: pip install spacy")
            
        # spaCy 모델 로드 - 벡터를 포함하는 모델로 변경 권장
        try:
            # 먼저 md 모델 시도 (벡터 포함)
            if settings.PDF_SPACY_LANGUAGE_MODEL == "en_core_web_sm":
                logger.warning("en_core_web_sm doesn't include word vectors. Consider using en_core_web_md or en_core_web_lg for better similarity calculation.")
                try:
                    self.nlp = spacy.load("en_core_web_md")
                    logger.info("Loaded en_core_web_md instead for better similarity calculation")
                except:
                    self.nlp = spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
            else:
                self.nlp = spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
        except:
            logger.warning(f"spaCy model {settings.PDF_SPACY_LANGUAGE_MODEL} not found. Installing...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", settings.PDF_SPACY_LANGUAGE_MODEL])
            self.nlp = spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
        
        # 전략별 설정 사용
        self.max_chunk_size = settings.PDF_SPACY_MAX_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_SPACY_MIN_CHUNK_SIZE
        self.topic_threshold = settings.PDF_SPACY_TOPIC_THRESHOLD
        
        # stop words 제거 옵션 추가
        self.filter_stop_words = True
        
        # 설정값 확인 로그
        logger.info(f"SpaCy Chunker initialized with:")
        logger.info(f"  - max_chunk_size: {self.max_chunk_size}")
        logger.info(f"  - min_chunk_size: {self.min_chunk_size}")
        logger.info(f"  - topic_threshold: {self.topic_threshold}")
        logger.info(f"  - language_model: {self.nlp.meta['name']}")
        logger.info(f"  - has_vectors: {self.nlp.meta.get('vectors', {}).get('vectors', 0) > 0}")
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """spaCy를 사용한 언어학적 청킹 (개선된 버전)"""
        
        # spaCy 문서 처리
        logger.info(f"Processing text with SpaCy, length: {len(text)} chars")
        doc = self.nlp(text)
        sentences = list(doc.sents)
        logger.info(f"SpaCy processing complete. Sentences found: {len(sentences)}")
        
        chunk_index = 0
        current_chunk = []
        current_entities = set()
        current_topics = set()
        chunk_start_char = 0
        
        # 청크의 대표 벡터 (문장들의 평균)
        current_chunk_vector = None
        
        for sent_idx, sent in enumerate(sentences):
            # 문장의 주요 엔티티와 명사 추출 (stop words 제외)
            sent_entities = self._extract_entities(sent)
            sent_topics = self._extract_topics(sent)
            
            # 현재 청크가 비어있으면 시작
            if not current_chunk:
                current_chunk = [sent]
                current_entities = sent_entities
                current_topics = sent_topics
                chunk_start_char = sent.start_char
                current_chunk_vector = sent.vector
                continue
            
            # 현재 청크 크기
            current_size = sum(len(s.text) for s in current_chunk)
            new_size = current_size + len(sent.text)
            
            # 의미적 유사도 계산 (개선된 방식)
            similarity_score = self._calculate_similarity(
                current_chunk, sent, 
                current_entities, sent_entities,
                current_topics, sent_topics,
                current_chunk_vector, sent.vector
            )
            
            # 분할 결정
            should_split = False
            
            # Case 1: 크기가 최대치를 초과하면 무조건 분할
            if new_size > self.max_chunk_size:
                should_split = True
                logger.debug(f"Split due to size limit: {new_size} > {self.max_chunk_size}")
            
            # Case 2: 유사도가 임계값 미만이고 최소 크기 이상이면 분할
            elif similarity_score < self.topic_threshold and current_size >= self.min_chunk_size:
                should_split = True
                logger.debug(f"Split due to low similarity: {similarity_score:.2f} < {self.topic_threshold}, size={current_size}")
            
            if should_split:
                # 현재 청크 저장
                chunk_text = ' '.join(s.text for s in current_chunk)
                
                yield self._create_chunk(
                    text=chunk_text,
                    chunk_index=chunk_index,
                    document_id=document_id,
                    metadata=metadata,
                    char_start=chunk_start_char,
                    char_end=current_chunk[-1].end_char,
                    entities=list(current_entities),
                    topics=list(current_topics),
                    avg_similarity=similarity_score
                )
                chunk_index += 1
                
                # 새 청크 시작
                current_chunk = [sent]
                current_entities = sent_entities
                current_topics = sent_topics
                chunk_start_char = sent.start_char
                current_chunk_vector = sent.vector
            else:
                # 현재 청크에 추가
                current_chunk.append(sent)
                current_entities.update(sent_entities)
                current_topics.update(sent_topics)
                # 청크 벡터 업데이트 (가중 평균)
                current_chunk_vector = self._update_chunk_vector(
                    current_chunk_vector, sent.vector, len(current_chunk)
                )
            
            # CPU 양보
            if sent_idx % 50 == 0:
                await asyncio.sleep(0)
        
        # 마지막 청크 처리
        if current_chunk:
            chunk_text = ' '.join(s.text for s in current_chunk)
            
            yield self._create_chunk(
                text=chunk_text,
                chunk_index=chunk_index,
                document_id=document_id,
                metadata=metadata,
                char_start=chunk_start_char,
                char_end=current_chunk[-1].end_char if current_chunk else chunk_start_char + len(chunk_text),
                entities=list(current_entities),
                topics=list(current_topics),
                avg_similarity=1.0  # 마지막 청크
            )
            chunk_index += 1
        
        # 디버깅 정보 출력
        logger.info(f"=== SpaCy Chunking Summary ===")
        logger.info(f"Document ID: {document_id}")
        logger.info(f"Total sentences: {len(sentences)}")
        logger.info(f"Total chunks created: {chunk_index}")
        logger.info(f"Settings - max: {self.max_chunk_size}, min: {self.min_chunk_size}, threshold: {self.topic_threshold}")
        logger.info(f"==============================")
    
    def _extract_entities(self, sent) -> Set[str]:
        """문장에서 엔티티 추출 (개선된 버전)"""
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
    
    def _extract_topics(self, sent) -> Set[str]:
        """문장에서 주제어 추출 (개선된 버전)"""
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
        stop_entities = {"the", "a", "an", "this", "that", "these", "those"}
        return text.lower() in stop_entities or len(text) < 2
    
    def _calculate_similarity(
        self,
        current_chunk: List,
        new_sent,
        current_entities: Set[str],
        sent_entities: Set[str],
        current_topics: Set[str],
        sent_topics: Set[str],
        current_vector,
        sent_vector
    ) -> float:
        """통합 유사도 계산 (개선된 버전)"""
        
        # 1. 엔티티 오버랩 (Jaccard similarity)
        entity_overlap = 0.0
        if current_entities or sent_entities:
            entity_overlap = len(current_entities & sent_entities) / len(current_entities | sent_entities)
        
        # 2. 토픽 오버랩 (Jaccard similarity)
        topic_overlap = 0.0
        if current_topics or sent_topics:
            topic_overlap = len(current_topics & sent_topics) / len(current_topics | sent_topics)
        
        # 3. 벡터 유사도 (모델이 벡터를 지원하는 경우)
        vector_similarity = 0.0
        if hasattr(new_sent, 'vector') and current_vector is not None:
            try:
                # 현재 청크의 평균 벡터와 새 문장의 벡터 비교
                vector_similarity = self._cosine_similarity(current_vector, sent_vector)
            except:
                vector_similarity = 0.0
        
        # 가중 평균 (벡터가 있으면 더 높은 가중치)
        if vector_similarity > 0:
            # 벡터 유사도가 있는 경우: 50% 벡터, 25% 엔티티, 25% 토픽
            similarity = (0.5 * vector_similarity + 
                         0.25 * entity_overlap + 
                         0.25 * topic_overlap)
        else:
            # 벡터가 없는 경우: 50% 엔티티, 50% 토픽
            similarity = (0.5 * entity_overlap + 0.5 * topic_overlap)
        
        return similarity
    
    def _cosine_similarity(self, vec1, vec2) -> float:
        """코사인 유사도 계산"""
        import numpy as np
        
        # numpy 배열로 변환
        v1 = np.array(vec1)
        v2 = np.array(vec2)
        
        # 정규화
        norm1 = np.linalg.norm(v1)
        norm2 = np.linalg.norm(v2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        # 코사인 유사도
        return np.dot(v1, v2) / (norm1 * norm2)
    
    def _update_chunk_vector(self, current_vector, new_vector, chunk_size: int):
        """청크 벡터 업데이트 (가중 평균)"""
        import numpy as np
        
        if current_vector is None:
            return new_vector
        
        # 가중 평균으로 업데이트
        weight_current = (chunk_size - 1) / chunk_size
        weight_new = 1 / chunk_size
        
        return weight_current * np.array(current_vector) + weight_new * np.array(new_vector)
    
    def _create_chunk(
        self,
        text: str,
        chunk_index: int,
        document_id: str,
        metadata: Dict[str, Any],
        char_start: int,
        char_end: int,
        entities: List[str] = None,
        topics: List[str] = None,
        avg_similarity: float = None
    ) -> ChunkDocument:
        """청크 문서 생성"""
        chunk_metadata = {
            **metadata,
            "chunking_strategy": "spacy_semantic",
            "chunk_size": len(text),
            "max_size_config": self.max_chunk_size,
            "min_size_config": self.min_chunk_size,
            "topic_threshold": self.topic_threshold,
            "entities": entities or [],
            "topics": topics or [],
            "entity_count": len(entities) if entities else 0,
            "topic_count": len(topics) if topics else 0
        }
        
        if avg_similarity is not None:
            chunk_metadata["avg_similarity"] = round(avg_similarity, 3)
        
        return ChunkDocument(
            document_id=document_id,
            chunk_id=str(uuid.uuid4()),
            chunk_index=chunk_index,
            text_content=text,
            char_start=char_start,
            char_end=char_end,
            chunk_metadata=chunk_metadata,
            created_at=datetime.now(timezone.utc)
        )
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수"""
        avg_chunk_size = (self.max_chunk_size + self.min_chunk_size) / 2
        return int(text_length / avg_chunk_size) + 1
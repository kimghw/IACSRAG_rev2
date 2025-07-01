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
from .spacy_utils import SpacyTextProcessor, SimilarityCalculator
from .spacy_chunk_tracker import ChunkTracker
from ..schema import ChunkDocument
from infra.core.config import settings


class SpacySemanticChunker(ChunkingStrategy):
    """spaCy 기반 언어학적 의미 청킹 (개선된 버전)"""
    
    def __init__(self):
        if not SPACY_AVAILABLE:
            raise ImportError("spaCy is not installed. Please install it with: pip install spacy")
        
        # spaCy 모델 로드
        self.nlp = self._load_spacy_model()
        
        # 전략별 설정 사용
        self.max_chunk_size = settings.PDF_SPACY_MAX_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_SPACY_MIN_CHUNK_SIZE
        self.topic_threshold = settings.PDF_SPACY_TOPIC_THRESHOLD
        
        # 유틸리티 클래스 초기화
        self.text_processor = SpacyTextProcessor()
        self.similarity_calculator = SimilarityCalculator()
        
        self._log_initialization()
    
    def _load_spacy_model(self):
        """spaCy 모델 로드"""
        try:
            # 먼저 md 모델 시도 (벡터 포함)
            if settings.PDF_SPACY_LANGUAGE_MODEL == "en_core_web_sm":
                logger.warning(
                    "en_core_web_sm doesn't include word vectors. "
                    "Consider using en_core_web_md or en_core_web_lg for better similarity calculation."
                )
                try:
                    nlp = spacy.load("en_core_web_md")
                    logger.info("Loaded en_core_web_md instead for better similarity calculation")
                    return nlp
                except:
                    pass
            
            return spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
            
        except:
            logger.warning(f"spaCy model {settings.PDF_SPACY_LANGUAGE_MODEL} not found. Installing...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", settings.PDF_SPACY_LANGUAGE_MODEL])
            return spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
    
    def _log_initialization(self):
        """초기화 정보 로깅"""
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
        """spaCy를 사용한 언어학적 청킹"""
        
        # 청크 추적기 초기화
        tracker = ChunkTracker(total_text_length=len(text))
        
        # spaCy 문서 처리
        logger.info(f"Processing text with SpaCy, length: {len(text)} chars")
        doc = self.nlp(text)
        sentences = list(doc.sents)
        logger.info(f"SpaCy processing complete. Sentences found: {len(sentences)}")
        
        # 청크 생성 상태
        chunk_state = ChunkState()
        
        for sent_idx, sent in enumerate(sentences):
            # 문장 처리
            sent_data = self._process_sentence(sent)
            
            # 청크 처리 결정
            if chunk_state.is_empty():
                chunk_state.start_new_chunk(sent, sent_data)
            else:
                should_split = self._should_split_chunk(chunk_state, sent, sent_data)
                
                if should_split:
                    # 현재 청크 생성 및 추적
                    chunk = chunk_state.create_chunk(document_id, metadata)
                    tracker.track_chunk(chunk)
                    yield chunk
                    
                    # 새 청크 시작
                    chunk_state.start_new_chunk(sent, sent_data)
                else:
                    # 현재 청크에 추가
                    chunk_state.add_to_chunk(sent, sent_data)
            
            # CPU 양보
            if sent_idx % 50 == 0:
                await asyncio.sleep(0)
        
        # 마지막 청크 처리
        if not chunk_state.is_empty():
            chunk = chunk_state.create_chunk(document_id, metadata, is_last=True)
            tracker.track_chunk(chunk)
            yield chunk
        
        # 텍스트 손실 검증 및 요약
        tracker.verify_coverage()
        tracker.log_summary(document_id)
    
    def _process_sentence(self, sent) -> Dict[str, Any]:
        """문장 처리 및 특징 추출"""
        return {
            'entities': self.text_processor.extract_entities(sent),
            'topics': self.text_processor.extract_topics(sent),
            'vector': sent.vector
        }
    
    def _should_split_chunk(self, chunk_state, sent, sent_data) -> bool:
        """청크 분할 여부 결정"""
        current_size = chunk_state.get_size()
        new_size = current_size + len(sent.text)
        
        # 크기 제한 확인
        if new_size > self.max_chunk_size:
            logger.debug(f"Split due to size limit: {new_size} > {self.max_chunk_size}")
            return True
        
        # 의미적 유사도 계산
        similarity = self.similarity_calculator.calculate(
            chunk_state.get_data(),
            sent_data,
            chunk_state.vector,
            sent.vector
        )
        
        # 유사도와 최소 크기 기준으로 분할 결정
        if similarity < self.topic_threshold and current_size >= self.min_chunk_size:
            logger.debug(f"Split due to low similarity: {similarity:.2f} < {self.topic_threshold}")
            return True
        
        return False
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수"""
        avg_chunk_size = (self.max_chunk_size + self.min_chunk_size) / 2
        return int(text_length / avg_chunk_size) + 1


class ChunkState:
    """청크 생성 상태 관리"""
    
    def __init__(self):
        self.sentences = []
        self.entities = set()
        self.topics = set()
        self.start_char = 0
        self.vector = None
        self.chunk_index = 0
    
    def is_empty(self) -> bool:
        return len(self.sentences) == 0
    
    def start_new_chunk(self, sent, sent_data):
        """새 청크 시작"""
        self.sentences = [sent]
        self.entities = sent_data['entities']
        self.topics = sent_data['topics']
        self.start_char = sent.start_char
        self.vector = sent_data['vector']
    
    def add_to_chunk(self, sent, sent_data):
        """현재 청크에 추가"""
        self.sentences.append(sent)
        self.entities.update(sent_data['entities'])
        self.topics.update(sent_data['topics'])
        self.vector = self._update_vector(self.vector, sent_data['vector'])
    
    def get_size(self) -> int:
        """현재 청크 크기"""
        return sum(len(s.text) for s in self.sentences)
    
    def get_data(self) -> Dict[str, Any]:
        """현재 청크 데이터"""
        return {
            'entities': self.entities,
            'topics': self.topics
        }
    
    def create_chunk(self, document_id: str, metadata: Dict[str, Any], is_last: bool = False) -> ChunkDocument:
        """청크 문서 생성"""
        chunk_text = ' '.join(s.text for s in self.sentences)
        end_char = self.sentences[-1].end_char if self.sentences else self.start_char + len(chunk_text)
        
        chunk_metadata = {
            **metadata,
            "chunking_strategy": "spacy_semantic",
            "chunk_size": len(chunk_text),
            "entities": list(self.entities),
            "topics": list(self.topics),
            "entity_count": len(self.entities),
            "topic_count": len(self.topics),
            "char_coverage": {
                "start": self.start_char,
                "end": end_char,
                "length": end_char - self.start_char
            }
        }
        
        chunk = ChunkDocument(
            document_id=document_id,
            chunk_id=str(uuid.uuid4()),
            chunk_index=self.chunk_index,
            text_content=chunk_text,
            char_start=self.start_char,
            char_end=end_char,
            chunk_metadata=chunk_metadata,
            created_at=datetime.now(timezone.utc)
        )
        
        self.chunk_index += 1
        return chunk
    
    def _update_vector(self, current_vector, new_vector):
        """벡터 업데이트 (가중 평균)"""
        import numpy as np
        
        if current_vector is None:
            return new_vector
        
        chunk_size = len(self.sentences)
        weight_current = (chunk_size - 1) / chunk_size
        weight_new = 1 / chunk_size
        
        return weight_current * np.array(current_vector) + weight_new * np.array(new_vector)
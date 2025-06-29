# content_pdf/chunking/spacy_semantic_chunker.py
import asyncio
from typing import List, AsyncGenerator, Dict, Any
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
    """spaCy 기반 언어학적 의미 청킹"""
    
    def __init__(self):
        if not SPACY_AVAILABLE:
            raise ImportError("spaCy is not installed. Please install it with: pip install spacy")
            
        # spaCy 모델 로드
        try:
            self.nlp = spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
        except:
            logger.warning(f"spaCy model {settings.PDF_SPACY_LANGUAGE_MODEL} not found. Installing...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", settings.PDF_SPACY_LANGUAGE_MODEL])
            self.nlp = spacy.load(settings.PDF_SPACY_LANGUAGE_MODEL)
        
        # 전략별 설정 사용
        self.max_chunk_size = settings.PDF_SPACY_MAX_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_SPACY_MIN_CHUNK_SIZE
        self.topic_keywords_threshold = settings.PDF_SPACY_TOPIC_THRESHOLD
        
        # 설정값 확인 로그
        logger.info(f"SpaCy Chunker initialized with:")
        logger.info(f"  - max_chunk_size: {self.max_chunk_size}")
        logger.info(f"  - min_chunk_size: {self.min_chunk_size}")
        logger.info(f"  - topic_threshold: {self.topic_keywords_threshold}")
        logger.info(f"  - language_model: {settings.PDF_SPACY_LANGUAGE_MODEL}")
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """spaCy를 사용한 언어학적 청킹"""
        
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
        prev_entities = set()
        prev_topics = set()
        
        for sent_idx, sent in enumerate(sentences):
            # 문장의 주요 엔티티와 명사 추출
            sent_entities = {ent.text.lower() for ent in sent.ents}
            sent_nouns = {token.text.lower() for token in sent if token.pos_ in ["NOUN", "PROPN"]}
            
            # 현재 청크가 비어있으면 시작
            if not current_chunk:
                current_chunk = [sent]
                current_entities = sent_entities
                current_topics = sent_nouns
                chunk_start_char = sent.start_char
                prev_entities = sent_entities.copy()
                prev_topics = sent_nouns.copy()
                continue
            
            # 현재 청크 크기
            current_size = sum(len(s.text) for s in current_chunk)
            new_size = current_size + len(sent.text)
            
            # 주제 연속성 계산 (이전 문장과 현재 문장 비교)
            entity_overlap = 1.0  # 기본값
            topic_overlap = 1.0   # 기본값
            
            if prev_entities or sent_entities:
                entity_overlap = len(prev_entities & sent_entities) / max(len(prev_entities | sent_entities), 1)
            
            if prev_topics or sent_nouns:
                topic_overlap = len(prev_topics & sent_nouns) / max(len(prev_topics | sent_nouns), 1)
            
            # 평균 유사도
            avg_overlap = (entity_overlap + topic_overlap) / 2
            
            # 분할 결정
            should_split = False
            
            # Case 1: 크기가 최대치를 초과하면 무조건 분할
            if new_size > self.max_chunk_size:
                should_split = True
                logger.debug(f"Split due to size limit: {new_size} > {self.max_chunk_size}")
            
            # Case 2: 주제가 크게 바뀌고 (임계값 미만) 최소 크기 이상이면 분할
            elif avg_overlap < self.topic_keywords_threshold and current_size >= self.min_chunk_size:
                should_split = True
                logger.debug(f"Split due to topic change: overlap={avg_overlap:.2f} < {self.topic_keywords_threshold}, size={current_size}")
            
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
                    topics=list(current_topics)
                )
                chunk_index += 1
                
                # 새 청크 시작
                current_chunk = [sent]
                current_entities = sent_entities
                current_topics = sent_nouns
                chunk_start_char = sent.start_char
            else:
                # 현재 청크에 추가
                current_chunk.append(sent)
                current_entities.update(sent_entities)
                current_topics.update(sent_nouns)
            
            # 다음 비교를 위해 현재 문장 정보 저장
            prev_entities = sent_entities.copy()
            prev_topics = sent_nouns.copy()
            
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
                topics=list(current_topics)
            )
            chunk_index += 1
        
        # 디버깅 정보 출력
        logger.info(f"=== SpaCy Chunking Summary ===")
        logger.info(f"Document ID: {document_id}")
        logger.info(f"Total sentences: {len(sentences)}")
        logger.info(f"Total chunks created: {chunk_index}")
        logger.info(f"Settings - max: {self.max_chunk_size}, min: {self.min_chunk_size}, threshold: {self.topic_keywords_threshold}")
        logger.info(f"==============================")
    
    def _create_chunk(
        self,
        text: str,
        chunk_index: int,
        document_id: str,
        metadata: Dict[str, Any],
        char_start: int,
        char_end: int,
        entities: List[str] = None,
        topics: List[str] = None
    ) -> ChunkDocument:
        """청크 문서 생성"""
        chunk_metadata = {
            **metadata,
            "chunking_strategy": "spacy_semantic",
            "chunk_size": len(text),
            "max_size_config": self.max_chunk_size,
            "min_size_config": self.min_chunk_size,
            "topic_threshold": self.topic_keywords_threshold,
            "entities": entities or [],
            "topics": topics or []
        }
        
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
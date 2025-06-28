# content_pdf/chunking/spacy_semantic_chunker.py
import spacy
from typing import List, AsyncGenerator, Dict, Any
from datetime import datetime, timezone
import uuid
import asyncio

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings
import logging

logger = logging.getLogger(__name__)

class SpacySemanticChunker(ChunkingStrategy):
    """spaCy 기반 언어학적 의미 청킹"""
    
    def __init__(self):
        # spaCy 모델 로드 (한국어: ko_core_news_sm, 영어: en_core_web_sm)
        try:
            self.nlp = spacy.load("en_core_web_sm")
        except:
            logger.warning("spaCy model not found. Installing...")
            import subprocess
            subprocess.run(["python", "-m", "spacy", "download", "en_core_web_sm"])
            self.nlp = spacy.load("en_core_web_sm")
        
        self.max_chunk_size = settings.PDF_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_CHUNK_SIZE // 4
        
        # 주제 변화 감지를 위한 설정
        self.topic_keywords_threshold = 0.3  # 주제 키워드 변화 임계값
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """spaCy를 사용한 언어학적 청킹"""
        
        # spaCy 문서 처리
        doc = self.nlp(text)
        
        chunk_index = 0
        current_chunk = []
        current_entities = set()
        current_topics = set()
        chunk_start_char = 0
        
        for sent_idx, sent in enumerate(doc.sents):
            # 문장의 주요 엔티티와 명사 추출
            sent_entities = {ent.text.lower() for ent in sent.ents}
            sent_nouns = {token.text.lower() for token in sent if token.pos_ in ["NOUN", "PROPN"]}
            
            # 현재 청크가 비어있으면 시작
            if not current_chunk:
                current_chunk = [sent]
                current_entities = sent_entities
                current_topics = sent_nouns
                chunk_start_char = sent.start_char
            else:
                # 주제 연속성 계산
                entity_overlap = len(current_entities & sent_entities) / max(len(current_entities | sent_entities), 1)
                topic_overlap = len(current_topics & sent_nouns) / max(len(current_topics | sent_nouns), 1)
                
                # 청크 크기 확인
                current_size = sum(len(s.text) for s in current_chunk)
                
                # 주제가 연속되고 크기가 허용되면 추가
                if ((entity_overlap > self.topic_keywords_threshold or 
                     topic_overlap > self.topic_keywords_threshold) and
                    current_size + len(sent.text) <= self.max_chunk_size):
                    
                    current_chunk.append(sent)
                    current_entities.update(sent_entities)
                    current_topics.update(sent_nouns)
                else:
                    # 청크 생성
                    if current_size >= self.min_chunk_size:
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
            
            # CPU 양보
            if sent_idx % 50 == 0:
                await asyncio.sleep(0)
        
        # 마지막 청크 처리
        if current_chunk and sum(len(s.text) for s in current_chunk) >= self.min_chunk_size:
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
        avg_chunk_size = self.max_chunk_size * 0.7
        return int(text_length / avg_chunk_size) + 1
# content_pdf/chunking/embedding_semantic_chunker.py
import numpy as np
import asyncio
import re
from typing import List, AsyncGenerator, Dict, Any
from datetime import datetime, timezone
import uuid

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings
import logging

logger = logging.getLogger(__name__)

class EmbeddingSemanticChunker(ChunkingStrategy):
    """임베딩 기반 의미적 청킹 전략"""
    
    def __init__(self, embedding_service, cache_embeddings: bool = True):
        self.embedding_service = embedding_service
        self.max_chunk_size = settings.PDF_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_CHUNK_SIZE // 4
        self.similarity_threshold = settings.PDF_SEMANTIC_SIMILARITY_THRESHOLD
        self.cache_embeddings = cache_embeddings
        self._embedding_cache = {} if cache_embeddings else None
        
        # 문장 분할 설정
        self.sentence_endings = r'[.!?][\s\n]'
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """임베딩을 사용한 의미적 청킹"""
        
        # 1. 문장 단위로 분할
        sentences = self._split_into_sentences(text)
        
        # 2. 각 문장의 임베딩 생성 (배치 처리)
        logger.info("Generating embeddings for semantic chunking...")
        sentence_embeddings = await self._generate_sentence_embeddings(sentences)
        
        # 3. 의미적 유사도 기반으로 청킹
        chunk_index = 0
        char_position = 0
        current_chunk = []
        current_chunk_embedding = None
        chunk_start = 0
        
        for i, (sentence, embedding) in enumerate(zip(sentences, sentence_embeddings)):
            if not sentence.strip():
                continue
            
            # 현재 청크가 비어있으면 시작
            if not current_chunk:
                current_chunk = [sentence]
                current_chunk_embedding = embedding
                chunk_start = char_position
            else:
                # 현재 청크와의 유사도 계산
                similarity = self._cosine_similarity(current_chunk_embedding, embedding)
                
                # 청크 크기와 유사도 기반 결정
                current_size = sum(len(s) for s in current_chunk)
                
                if (similarity >= self.similarity_threshold and 
                    current_size + len(sentence) <= self.max_chunk_size):
                    # 유사하고 크기가 허용되면 현재 청크에 추가
                    current_chunk.append(sentence)
                    # 청크 임베딩 업데이트 (평균)
                    current_chunk_embedding = self._update_chunk_embedding(
                        current_chunk_embedding, embedding, len(current_chunk)
                    )
                else:
                    # 청크 생성
                    if current_size >= self.min_chunk_size:
                        chunk_text = ' '.join(current_chunk)
                        yield self._create_chunk(
                            text=chunk_text,
                            chunk_index=chunk_index,
                            document_id=document_id,
                            metadata=metadata,
                            char_start=chunk_start,
                            char_end=chunk_start + len(chunk_text)
                        )
                        chunk_index += 1
                    
                    # 새 청크 시작
                    current_chunk = [sentence]
                    current_chunk_embedding = embedding
                    chunk_start = char_position
            
            char_position += len(sentence)
            
            # CPU 양보
            if i % 100 == 0:
                await asyncio.sleep(0)
        
        # 마지막 청크 처리
        if current_chunk and sum(len(s) for s in current_chunk) >= self.min_chunk_size:
            chunk_text = ' '.join(current_chunk)
            yield self._create_chunk(
                text=chunk_text,
                chunk_index=chunk_index,
                document_id=document_id,
                metadata=metadata,
                char_start=chunk_start,
                char_end=chunk_start + len(chunk_text)
            )
    
    async def _generate_sentence_embeddings(self, sentences: List[str]) -> List[np.ndarray]:
        """문장들의 임베딩 생성"""
        # 빈 문장 제거
        non_empty_sentences = [s for s in sentences if s.strip()]
        
        if not non_empty_sentences:
            return []
        
        # 배치로 임베딩 생성
        batch_size = 100
        all_embeddings = []
        
        for i in range(0, len(non_empty_sentences), batch_size):
            batch = non_empty_sentences[i:i + batch_size]
            
            try:
                # 임시 청크 객체 생성 (embedding service 인터페이스 맞추기)
                temp_chunks = [
                    ChunkDocument(
                        document_id="temp",
                        chunk_id=str(uuid.uuid4()),
                        chunk_index=idx,
                        text_content=text,
                        char_start=0,
                        char_end=len(text),
                        chunk_metadata={},
                        created_at=datetime.now(timezone.utc)
                    )
                    for idx, text in enumerate(batch)
                ]
                
                # 임베딩 생성
                embeddings = await self.embedding_service.generate_embeddings_batch(temp_chunks)
                
                if not embeddings:
                    logger.error(f"No embeddings returned for batch {i//batch_size}")
                    # 제로 벡터로 대체
                    for _ in batch:
                        all_embeddings.append(np.zeros(settings.QDRANT_VECTOR_SIZE))
                else:
                    # numpy 배열로 변환
                    for emb in embeddings:
                        all_embeddings.append(np.array(emb.embedding_vector))
                        
            except Exception as e:
                logger.error(f"Failed to generate embeddings for batch {i//batch_size}: {e}")
                # 실패한 배치는 제로 벡터로 대체
                for _ in batch:
                    all_embeddings.append(np.zeros(settings.QDRANT_VECTOR_SIZE))
        
        # 원래 문장 순서대로 매핑
        result = []
        emb_idx = 0
        for sentence in sentences:
            if sentence.strip():
                result.append(all_embeddings[emb_idx])
                emb_idx += 1
            else:
                result.append(np.zeros(settings.QDRANT_VECTOR_SIZE))
        
        return result
    
    def _cosine_similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """코사인 유사도 계산"""
        dot_product = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        
        if norm1 == 0 or norm2 == 0:
            return 0.0
        
        return dot_product / (norm1 * norm2)
    
    def _update_chunk_embedding(
        self, 
        current_embedding: np.ndarray, 
        new_embedding: np.ndarray, 
        chunk_size: int
    ) -> np.ndarray:
        """청크 임베딩 업데이트 (가중 평균)"""
        # 기존 임베딩의 가중치
        weight_current = (chunk_size - 1) / chunk_size
        weight_new = 1 / chunk_size
        
        return weight_current * current_embedding + weight_new * new_embedding
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """텍스트를 문장으로 분할 - 개선된 버전"""
        # 약어 처리 (Mr., Dr., etc.)
        text = re.sub(r'\b(Mr|Mrs|Dr|Ms|Prof|Sr|Jr)\.', r'\1<prd>', text)
        
        # 회사명 약어 처리 (Inc., Ltd., etc.)
        text = re.sub(r'\b(Inc|Ltd|Corp|Co)\.', r'\1<prd>', text)
        
        # 월 약어 처리 (Jan., Feb., etc.)
        text = re.sub(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.', r'\1<prd>', text)
        
        # 숫자 + 마침표 처리 (예: 3.14)
        text = re.sub(r'(\d+)\.(\d+)', r'\1<prd>\2', text)
        
        # 문장 분할
        sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z])', text)
        
        # <prd>를 다시 마침표로
        sentences = [s.replace('<prd>', '.') for s in sentences]
        
        # 빈 문장 제거 및 공백 정리
        return [s.strip() for s in sentences if s.strip()]
    
    def _create_chunk(
        self,
        text: str,
        chunk_index: int,
        document_id: str,
        metadata: Dict[str, Any],
        char_start: int,
        char_end: int
    ) -> ChunkDocument:
        """청크 문서 생성"""
        return ChunkDocument(
            document_id=document_id,
            chunk_id=str(uuid.uuid4()),
            chunk_index=chunk_index,
            text_content=text,
            char_start=char_start,
            char_end=char_end,
            chunk_metadata={
                **metadata,
                "chunking_strategy": "embedding_semantic",
                "chunk_size": len(text),
                "similarity_threshold": self.similarity_threshold
            },
            created_at=datetime.now(timezone.utc)
        )
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수"""
        # 의미적 청킹은 예측이 어려우므로 대략적 추정
        avg_chunk_size = self.max_chunk_size * 0.6
        return int(text_length / avg_chunk_size) + 1

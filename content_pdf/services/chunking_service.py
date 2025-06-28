# content_pdf/services/chunking_service.py
import asyncio
from typing import Dict, Any, Optional
import logging

from ..chunking.base import ChunkingStrategy
from ..chunking.character_chunker import CharacterChunker
from ..chunking.token_chunker import TokenChunker
from ..chunking.semantic_chunker import SemanticChunker
from ..chunking.embedding_semantic_chunker import EmbeddingSemanticChunker
from ..chunking.spacy_semantic_chunker import SpacySemanticChunker
from ..chunking.recursive_semantic_chunker import RecursiveSemanticChunker
from .batch_processor import BatchProcessor
from infra.core.config import settings
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class ChunkingService:
    """청킹 + 배치 처리 통합 서비스"""
    
    def __init__(self, strategy: str = None, embedding_service=None):
        self.strategy_name = strategy or settings.PDF_CHUNKING_STRATEGY
        self.embedding_service = embedding_service
        self.strategy = self._create_strategy(self.strategy_name)
        self.batch_size = settings.PDF_BATCH_SIZE
        
    def _create_strategy(self, strategy_name: str) -> ChunkingStrategy:
        """청킹 전략 팩토리"""
        strategies = {
            "character": CharacterChunker,
            "token": TokenChunker,
            "semantic": SemanticChunker,
            "recursive_semantic": RecursiveSemanticChunker,
            "spacy_semantic": SpacySemanticChunker,
            "embedding_semantic": lambda: EmbeddingSemanticChunker(self.embedding_service)
        }
        
        if strategy_name not in strategies:
            logger.warning(f"Unknown strategy '{strategy_name}', falling back to 'character'")
            strategy_name = "character"
        
        # embedding_semantic은 embedding_service가 필요
        if strategy_name == "embedding_semantic":
            if not self.embedding_service:
                logger.warning("embedding_semantic requires embedding_service, falling back to 'recursive_semantic'")
                strategy_name = "recursive_semantic"
            else:
                return strategies[strategy_name]()
        
        # 다른 전략들
        strategy_class = strategies[strategy_name]
        return strategy_class() if callable(strategy_class) else strategy_class
    
    async def process_with_embeddings(
        self,
        document_id: str,
        text: str,
        metadata: Dict[str, Any],
        embedding_service,
        storage_service
    ) -> Dict[str, Any]:
        """청킹 → 배치 → 임베딩 → 저장 파이프라인"""
        
        # embedding_service 설정 (embedding_semantic 전략용)
        if self.strategy_name == "embedding_semantic" and not self.embedding_service:
            self.embedding_service = embedding_service
            self.strategy = self._create_strategy(self.strategy_name)
        
        # 청킹 시작 로그
        processing_logger.chunking_started(document_id)
        chunking_start = asyncio.get_event_loop().time()
        
        # 배치 프로세서 생성
        batch_processor = BatchProcessor(
            batch_size=self.batch_size,
            embedding_service=embedding_service,
            storage_service=storage_service,
            document_id=document_id
        )
        
        # 스트리밍 청킹하면서 배치 처리
        chunk_count = 0
        async for chunk in self.strategy.chunk_streaming(text, document_id, metadata):
            await batch_processor.add_chunk(chunk)
            chunk_count += 1
            
            # 진행 상황 로깅 (100개마다)
            if chunk_count % 100 == 0:
                logger.info(f"Processed {chunk_count} chunks...")
        
        # 남은 청크 처리
        final_stats = await batch_processor.flush()
        final_stats['total_chunks'] = chunk_count
        final_stats['strategy'] = self.strategy_name
        
        # 청킹 완료 로그
        chunking_duration = asyncio.get_event_loop().time() - chunking_start
        processing_logger.chunking_completed(
            document_id=document_id,
            duration=chunking_duration,
            chunk_count=chunk_count
        )
        
        logger.info(f"Chunking completed with strategy '{self.strategy_name}': {chunk_count} chunks")
        
        return final_stats
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """현재 전략 정보 반환"""
        return {
            "name": self.strategy_name,
            "class": self.strategy.__class__.__name__,
            "batch_size": self.batch_size,
            "available_strategies": [
                "character",
                "token", 
                "semantic",
                "recursive_semantic",
                "spacy_semantic",
                "embedding_semantic"
            ]
        }
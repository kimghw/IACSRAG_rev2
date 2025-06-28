# content_pdf/services/chunking_service.py
import asyncio
from typing import Dict, Any
import logging

from ..chunking.base import ChunkingStrategy
from ..chunking.character_chunker import CharacterChunker
from ..chunking.token_chunker import TokenChunker
from ..chunking.semantic_chunker import SemanticChunker
from .batch_processor import BatchProcessor
from infra.core.config import settings
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class ChunkingService:
    """청킹 + 배치 처리 통합 서비스"""
    
    def __init__(self, strategy: str = None):
        self.strategy_name = strategy or settings.PDF_CHUNKING_STRATEGY
        self.strategy = self._create_strategy(self.strategy_name)
        self.batch_size = settings.PDF_BATCH_SIZE
        
    def _create_strategy(self, strategy_name: str) -> ChunkingStrategy:
        """청킹 전략 팩토리"""
        strategies = {
            "character": CharacterChunker,
            "token": TokenChunker,
            "semantic": SemanticChunker
        }
        
        if strategy_name not in strategies:
            logger.warning(f"Unknown strategy '{strategy_name}', falling back to 'character'")
            strategy_name = "character"
        
        return strategies[strategy_name]()
    
    async def process_with_embeddings(
        self,
        document_id: str,
        text: str,
        metadata: Dict[str, Any],
        embedding_service,
        storage_service
    ) -> Dict[str, Any]:
        """청킹 → 배치 → 임베딩 → 저장 파이프라인"""
        
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
        
        # 청킹 완료 로그
        chunking_duration = asyncio.get_event_loop().time() - chunking_start
        processing_logger.chunking_completed(
            document_id=document_id,
            duration=chunking_duration,
            chunk_count=chunk_count
        )
        
        return final_stats
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """현재 전략 정보 반환"""
        return {
            "name": self.strategy_name,
            "class": self.strategy.__class__.__name__,
            "batch_size": self.batch_size
        }

# content_pdf/pdf_chunker.py
from typing import AsyncGenerator, Dict, Any
import logging
from datetime import datetime, timezone
import uuid
import gc

from .schema import ChunkDocument
from .chunking.base import ChunkingStrategy
from .chunking.character_chunker import CharacterChunker
from .chunking.token_chunker import TokenChunker
from .chunking.semantic_chunker import SemanticChunker
from infra.core.config import settings

logger = logging.getLogger(__name__)

class PdfChunker:
    """전략 패턴을 사용한 PDF 청킹 서비스"""
    
    def __init__(self, strategy: str = None):
        self.strategy_name = strategy or settings.PDF_CHUNKING_STRATEGY
        self.strategy = self._create_strategy(self.strategy_name)
        logger.info(f"Initialized PdfChunker with strategy: {self.strategy_name}")
    
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
        
        try:
            return strategies[strategy_name]()
        except Exception as e:
            logger.error(f"Failed to create {strategy_name} strategy: {e}")
            # 폴백으로 character 전략 사용
            return CharacterChunker()
    
    async def create_chunks_streaming(
        self,
        document_id: str,
        text: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """선택된 전략으로 스트리밍 청킹"""
        
        # 메타데이터에 청킹 전략 추가
        enhanced_metadata = {
            **metadata,
            "chunking_strategy": self.strategy_name
        }
        
        # 예상 청크 수 로깅
        estimated_chunks = self.strategy.estimate_chunks(len(text))
        logger.info(f"Starting {self.strategy_name} chunking for document {document_id}, estimated chunks: {estimated_chunks}")
        
        chunk_count = 0
        
        try:
            # 전략에 따른 청킹 실행
            async for chunk in self.strategy.chunk_streaming(
                text=text,
                document_id=document_id,
                metadata=enhanced_metadata
            ):
                yield chunk
                chunk_count += 1
                
                # 주기적으로 메모리 정리
                if chunk_count % 100 == 0:
                    gc.collect()
                    logger.debug(f"Processed {chunk_count} chunks so far...")
                    
        except Exception as e:
            logger.error(f"Chunking failed after {chunk_count} chunks: {e}")
            raise
        
        logger.info(f"Chunking completed: {chunk_count} chunks created")
    
    def get_strategy_info(self) -> Dict[str, Any]:
        """현재 전략 정보 반환"""
        info = {
            "name": self.strategy_name,
            "class": self.strategy.__class__.__name__,
            "settings": {}
        }
        
        # 전략별 설정 정보
        if self.strategy_name == "character":
            info["settings"] = {
                "chunk_size": settings.PDF_CHUNK_SIZE,
                "overlap": settings.PDF_CHUNK_OVERLAP
            }
        elif self.strategy_name == "token":
            info["settings"] = {
                "model": settings.PDF_TOKEN_MODEL,
                "max_tokens": settings.PDF_MAX_TOKENS_PER_CHUNK,
                "overlap": settings.PDF_TOKEN_OVERLAP
            }
        elif self.strategy_name == "semantic":
            info["settings"] = {
                "threshold": settings.PDF_SEMANTIC_SIMILARITY_THRESHOLD,
                "max_size": settings.PDF_CHUNK_SIZE,
                "model": settings.PDF_SEMANTIC_MODEL
            }
        
        return info
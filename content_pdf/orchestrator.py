# content_pdf/orchestrator.py
import asyncio
from typing import Dict, Any, Optional
from datetime import datetime, timezone
import logging

from .services.text_extractor import TextExtractor  # 수정된 import
from .services.chunking_service import ChunkingService
from .services.embedding_service import EmbeddingService
from .services.storage_service import StorageService
from .schema import ProcessingRequest  # 범용 스키마로 변경
from schema import ProcessedContent
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class ContentOrchestrator:
    """PDF/Markdown 처리 플로우 조정자 - 텍스트 기반 콘텐츠 통합 처리"""
    
    def __init__(self, chunking_strategy: str = None):
        self.text_extractor = TextExtractor()  # 범용 텍스트 추출기
        self.chunking_service = ChunkingService(strategy=chunking_strategy)
        self.embedding_service = EmbeddingService()
        self.storage_service = StorageService()
        
        # 전략 정보 로깅
        strategy_info = self.chunking_service.get_strategy_info()
        logger.info(f"Content Orchestrator initialized with strategy: {strategy_info}")
    
    async def process_content(self, request: ProcessingRequest) -> ProcessedContent:
        """텍스트 기반 콘텐츠 처리 메인 플로우"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # 상태 업데이트: 처리 시작
            await self.storage_service.update_status(
                request.document_id, 
                "processing"
            )
            
            # 1단계: 텍스트 추출 (PDF/Markdown 자동 구분)
            processing_logger.text_extraction_started(request.document_id)
            text, content_type = await self.text_extractor.extract(request.document_id)
            processing_logger.text_extraction_completed(
                document_id=request.document_id,
                duration=asyncio.get_event_loop().time() - start_time,
                text_length=len(text)
            )
            
            # 메타데이터에 실제 처리된 콘텐츠 타입 추가
            request.metadata['processed_content_type'] = content_type
            
            # 2단계: 청킹 및 임베딩 (스트리밍 + 배치)
            processing_stats = await self.chunking_service.process_with_embeddings(
                document_id=request.document_id,
                text=text,
                metadata=request.metadata,
                embedding_service=self.embedding_service,
                storage_service=self.storage_service
            )
            
            # 3단계: 처리 완료
            total_duration = asyncio.get_event_loop().time() - start_time
            processing_logger.processing_completed(
                document_id=request.document_id,
                total_duration=total_duration,
                stats=processing_stats
            )
            
            # 상태 업데이트: 완료
            await self.storage_service.update_status(
                request.document_id, 
                "completed",
                processing_info=processing_stats
            )
            
            return ProcessedContent(
                content_id=request.document_id,
                original_type=content_type,  # pdf 또는 markdown
                processed_data=processing_stats,
                metadata=request.metadata,
                processed_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            # 에러 로깅
            processing_logger.error(
                document_id=request.document_id,
                step="content_processing",
                error=str(e)
            )
            
            # 상태 업데이트: 실패
            await self.storage_service.update_status(
                request.document_id, 
                "failed", 
                error_message=str(e)
            )
            raise
    
    # 기존 process_pdf 메서드는 하위 호환성을 위해 유지
    async def process_pdf(self, request: ProcessingRequest) -> ProcessedContent:
        """PDF 처리 - process_content로 위임"""
        return await self.process_content(request)
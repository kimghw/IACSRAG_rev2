# content_pdf/orchestrator.py
import asyncio
from typing import Dict, Any
from datetime import datetime, timezone
import logging

from .services.text_extractor import PdfTextExtractor
from .services.chunking_service import ChunkingService
from .services.embedding_service import EmbeddingService
from .services.storage_service import StorageService
from .schema import PdfProcessingRequest
from schema import ProcessedContent
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class PdfOrchestrator:
    """PDF 처리 플로우 조정자 - 단계만 관리"""
    
    def __init__(self, chunking_strategy: str = None):
        self.text_extractor = PdfTextExtractor()
        self.chunking_service = ChunkingService(strategy=chunking_strategy)
        self.embedding_service = EmbeddingService()
        self.storage_service = StorageService()
        
        # 전략 정보 로깅
        strategy_info = self.chunking_service.get_strategy_info()
        logger.info(f"PDF Orchestrator initialized with strategy: {strategy_info}")
    
    async def process_pdf(self, request: PdfProcessingRequest) -> ProcessedContent:
        """PDF 처리 메인 플로우 - 단계만 표시"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            # 상태 업데이트: 처리 시작
            await self.storage_service.update_status(
                request.document_id, 
                "processing"
            )
            
            # 1단계: 텍스트 추출
            processing_logger.text_extraction_started(request.document_id)
            text = await self.text_extractor.extract(request.document_id)
            processing_logger.text_extraction_completed(
                document_id=request.document_id,
                duration=asyncio.get_event_loop().time() - start_time,
                text_length=len(text)
            )
            
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
                original_type="pdf",
                processed_data=processing_stats,
                metadata=request.metadata,
                processed_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            # 에러 로깅
            processing_logger.error(
                document_id=request.document_id,
                step="pdf_processing",
                error=str(e)
            )
            
            # 상태 업데이트: 실패
            await self.storage_service.update_status(
                request.document_id, 
                "failed", 
                error_message=str(e)
            )
            raise
# content_email/orchestrator.py
import logging
import time
from typing import Dict, Any

from .services.email_processor import EmailProcessor
from .services.email_embedding_service import EmailEmbeddingService
from .services.email_attachment_service import EmailAttachmentService
from .services.email_storage_service import EmailStorageService
from .schema import EmailProcessingRequest, EmailProcessingResult
from infra.core.config import settings

logger = logging.getLogger(__name__)

class EmailOrchestrator:
    """이메일 처리 오케스트레이터 - 흐름 제어만 담당"""
    
    def __init__(self):
        self.processor = EmailProcessor()
        self.embedding_service = EmailEmbeddingService()
        self.attachment_service = EmailAttachmentService()
        self.storage_service = EmailStorageService()
        
        # 배치 설정
        self.batch_size = settings.EMAIL_BATCH_SIZE
        self.max_concurrent_batches = settings.EMAIL_MAX_CONCURRENT_BATCHES
    
    async def process_email(self, request: EmailProcessingRequest) -> EmailProcessingResult:
        """이메일 이벤트 처리 메인 플로우"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting email processing for event: {request.event_id}")
            
            # 1. 이메일 목록 추출
            emails = self._extract_emails(request.event_data)
            logger.info(f"Processing {len(emails)} emails")
            
            # 2. 배치로 처리
            processing_stats = await self.processor.process_emails_with_embeddings(
                emails=emails,
                account_id=request.account_id,
                event_id=request.event_id,
                batch_size=self.batch_size,
                max_concurrent=self.max_concurrent_batches,
                embedding_service=self.embedding_service,
                attachment_service=self.attachment_service,
                storage_service=self.storage_service
            )
            
            # 3. 처리 결과 생성
            processing_time = time.time() - start_time
            
            return EmailProcessingResult(
                event_id=request.event_id,
                account_id=request.account_id,
                email_count=len(emails),
                processed_emails=processing_stats['processed_emails'],
                total_attachments=processing_stats['total_attachments'],
                downloaded_attachments=processing_stats['downloaded_attachments'],
                processing_time=processing_time
            )
            
        except Exception as e:
            logger.error(f"Email processing failed: {str(e)}", exc_info=True)
            # 상태 업데이트
            await self.storage_service.update_event_status(
                event_id=request.event_id,
                status="failed",
                error_message=str(e)
            )
            raise
    
    def _extract_emails(self, event_data: Dict[str, Any]) -> list:
        """이벤트 데이터에서 이메일 목록 추출"""
        response_data = event_data.get('response_data', {})
        return response_data.get('value', [])
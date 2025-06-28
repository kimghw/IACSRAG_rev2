# upload_service/orchestrator.py
import time
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

from .repository import UploadRepository
from .event_publisher import EventPublisher
from .file_hasher import FileHasher
from .schema import UploadRequest, UploadResponse
from schema import ProcessingStatus, DocumentEventType, DocumentUploadedEvent
from infra.core.processing_logger import processing_logger
from infra.core.config import settings
import logging

logger = logging.getLogger(__name__)

class UploadOrchestrator:
    def __init__(self):
        self.repository = UploadRepository()
        self.event_publisher = EventPublisher()
        # 동시 업로드 처리 제한
        self.max_concurrent_uploads = 10
        self.upload_semaphore = asyncio.Semaphore(self.max_concurrent_uploads)
    
    def _determine_event_type(self, filename: str) -> DocumentEventType:
        """파일 확장자를 기반으로 이벤트 타입 결정"""
        suffix = Path(filename).suffix.lower()
        
        if suffix == '.pdf':
            return DocumentEventType.PDF
        elif suffix in ['.md', '.markdown']:
            return DocumentEventType.MARKDOWN
        elif suffix == '.json':
            return DocumentEventType.JSON
        else:
            raise ValueError(f"Unsupported file type: {suffix}")
    
    async def process_upload(self, upload_request: UploadRequest, file_content: bytes) -> UploadResponse:
        """
        단일 파일 업로드 처리
        1. 파일 해시 생성 (설정에 따라)
        2. MongoDB에 파일 저장 (GridFS)
        3. 메타데이터 저장
        4. 파일 타입에 맞는 이벤트 발행
        """
        async with self.upload_semaphore:  # 동시 업로드 수 제한
            start_time = time.time()
            
            # 업로드 시작 로그
            processing_logger.upload_started(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                file_size=upload_request.file_size
            )
            
            try:
                # 파일 해시 생성 (설정에 따라)
                quick_hash = None
                if settings.UPLOAD_APPLY_HASHER:
                    quick_hash = FileHasher.generate_quick_hash(
                        upload_request.filename, 
                        file_content
                    )
                    logger.info(f"Generated quick hash: {quick_hash[:16]}... for {upload_request.filename}")
                    
                    # 중복 파일 확인 (선택사항)
                    existing = await self.repository.find_by_hash(quick_hash)
                    if existing:
                        logger.warning(f"Duplicate file detected with hash: {quick_hash[:16]}...")
                        logger.warning(f"Existing document: {existing.get('document_id')}")
                
                # 파일 타입에 따른 이벤트 타입 결정
                event_type = self._determine_event_type(upload_request.filename)
                logger.info(f"Determined event type: {event_type.value} for file: {upload_request.filename}")
                
                # MongoDB에 파일과 메타데이터 저장 (해시 포함)
                await self.repository.save_upload_with_file(
                    upload_request, 
                    file_content,
                    quick_hash
                )
                
                # 이벤트 발행 - 파일 타입별로 구분
                event = DocumentUploadedEvent(
                    event_type=event_type,
                    document_id=upload_request.document_id,
                    filename=upload_request.filename,
                    content_type=upload_request.content_type,
                    file_size=upload_request.file_size,
                    metadata=upload_request.metadata,
                    uploaded_at=upload_request.uploaded_at,
                    event_timestamp=datetime.now(timezone.utc)
                )
                
                await self.event_publisher.publish_document_uploaded(event)
                logger.info(f"Published {event_type.value} event for document: {upload_request.document_id}")
                
                # 업로드 완료 로그
                duration = time.time() - start_time
                processing_logger.upload_completed(
                    document_id=upload_request.document_id,
                    duration=duration
                )
                
                return UploadResponse(
                    document_id=upload_request.document_id,
                    status=ProcessingStatus.UPLOADED,
                    message=f"File uploaded successfully with event type: {event_type.value}",
                    uploaded_at=upload_request.uploaded_at,
                    metadata=upload_request.metadata
                )
                
            except ValueError as e:
                # 지원하지 않는 파일 타입
                processing_logger.error(
                    document_id=upload_request.document_id,
                    step="upload",
                    error=str(e)
                )
                raise
                
            except Exception as e:
                processing_logger.error(
                    document_id=upload_request.document_id,
                    step="upload",
                    error=str(e)
                )
                await self.repository.update_upload_status(
                    document_id=upload_request.document_id,
                    status="failed",
                    error_message=str(e)
                )
                raise
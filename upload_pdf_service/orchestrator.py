#from typing import Dict, Any
import os
from datetime import datetime

from .upload_handler import UploadHandler
from .event_publisher import EventPublisher
from .repository import UploadRepository
from schema import UploadRequest, UploadResponse, DocumentUploadedEvent
from infra.core.config import settings

class UploadOrchestrator:
    """업로드 처리 오케스트레이터 - 단순하고 명확한 플로우"""
    
    def __init__(self):
        self.handler = UploadHandler()
        self.publisher = EventPublisher()
        self.repository = UploadRepository()
    
    async def process_upload(
        self, 
        upload_request: UploadRequest,
        file_content: bytes
    ) -> UploadResponse:
        """
        업로드 처리 메인 플로우
        1. 임시 파일 저장
        2. 업로드 이력 저장
        3. 이벤트 발행
        4. 응답 반환
        """
        try:
            # 1. 임시 파일 저장
            file_path = await self.handler.save_temp_file(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                file_content=file_content
            )
            
            # 2. 업로드 이력 저장
            await self.repository.save_upload_record(upload_request)
            
            # 3. 이벤트 발행
            event = DocumentUploadedEvent(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                content_type=upload_request.content_type,
                file_size=upload_request.file_size,
                file_path=file_path,
                metadata=upload_request.metadata,
                uploaded_at=upload_request.uploaded_at,
                event_timestamp=datetime.utcnow()
            )
            
            await self.publisher.publish_document_uploaded(event)
            
            # 4. 응답 반환
            return UploadResponse(
                document_id=upload_request.document_id,
                status="uploaded",
                message="File uploaded successfully and queued for processing",
                uploaded_at=upload_request.uploaded_at,
                metadata=upload_request.metadata
            )
            
        except Exception as e:
            # 실패 시 상태 업데이트
            await self.repository.update_upload_status(
                document_id=upload_request.document_id,
                status="failed",
                error_message=str(e)
            )
            raise
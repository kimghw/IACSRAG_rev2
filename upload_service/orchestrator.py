#from typing import Dict, Any
import os
from datetime import datetime, timezone

from .upload_handler import UploadHandler
from .event_publisher import EventPublisher
from .repository import UploadRepository
from schema import UploadRequest, UploadResponse, DocumentUploadedEvent
from infra.core.config import settings

# upload_service/orchestrator.py 수정
class UploadOrchestrator:
    def __init__(self):
        # self.handler = UploadHandler()  # 제거
        self.publisher = EventPublisher()
        self.repository = UploadRepository()
    
    async def process_upload(self, upload_request: UploadRequest, file_content: bytes) -> UploadResponse:
        # 1. 파일시스템 저장 제거
        # file_path = await self.handler.save_temp_file(...)  # 제거
        
        # 2. MongoDB에 파일 포함해서 저장
        await self.repository.save_upload_with_file(upload_request, file_content)
        
        # 3. 이벤트 발행 (file_path 대신 document_id만)
        event = DocumentUploadedEvent(
            document_id=upload_request.document_id,
            filename=upload_request.filename,
            content_type=upload_request.content_type,
            file_size=upload_request.file_size,
            # file_path=file_path,  # 제거
            metadata=upload_request.metadata,
            uploaded_at=upload_request.uploaded_at,
            event_timestamp=datetime.now(timezone.utc)
        )

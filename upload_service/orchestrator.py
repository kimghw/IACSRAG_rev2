# upload_service/orchestrator.py
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from .repository import UploadRepository
from .event_publisher import EventPublisher
from .schema import UploadRequest, UploadResponse  # 모듈 내부에서
from schema import DocumentUploadedEvent, ProcessingStatus  # 루트에서

class UploadOrchestrator:
    def __init__(self):
        self.publisher = EventPublisher()
        self.repository = UploadRepository()
    
    async def process_upload(self, upload_request: UploadRequest, file_content: bytes) -> UploadResponse:
        try:
            # 1. MongoDB에 파일과 메타데이터 저장
            await self.repository.save_upload_with_file(upload_request, file_content)
            
            # 2. 이벤트 발행 (file_path 제거)
            event = DocumentUploadedEvent(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                content_type=upload_request.content_type,
                file_size=upload_request.file_size,
                metadata=upload_request.metadata,
                uploaded_at=upload_request.uploaded_at,
                event_timestamp=datetime.now(timezone.utc)
            )
            
            await self.publisher.publish_document_uploaded(event)
            
            return UploadResponse(
                document_id=upload_request.document_id,
                status=ProcessingStatus.UPLOADED,
                message="File uploaded successfully",
                uploaded_at=upload_request.uploaded_at,
                metadata=upload_request.metadata
            )
            
        except Exception as e:
            await self.repository.update_upload_status(
                document_id=upload_request.document_id,
                status="failed",
                error_message=str(e)
            )
            raise
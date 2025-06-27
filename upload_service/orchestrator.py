# upload_service/orchestrator.py - 간소화 버전
import time
from datetime import datetime, timezone
from .repository import UploadRepository
from .schema import UploadRequest, UploadResponse
from schema import ProcessingStatus
from infra.core.processing_logger import processing_logger

class UploadOrchestrator:
    def __init__(self):
        self.repository = UploadRepository()
        # event_publisher 제거 - 필요 없음
    
    async def process_upload(self, upload_request: UploadRequest, file_content: bytes) -> UploadResponse:
        start_time = time.time()
        
        # 업로드 시작 로그
        processing_logger.upload_started(
            document_id=upload_request.document_id,
            filename=upload_request.filename,
            file_size=upload_request.file_size
        )
        
        try:
            # MongoDB에 파일과 메타데이터 저장
            await self.repository.save_upload_with_file(upload_request, file_content)
            
            # 이벤트 발행 제거 - Consumer가 없으므로 불필요
            
            # 업로드 완료 로그
            duration = time.time() - start_time
            processing_logger.upload_completed(
                document_id=upload_request.document_id,
                duration=duration
            )
            
            return UploadResponse(
                document_id=upload_request.document_id,
                status=ProcessingStatus.UPLOADED,
                message="File uploaded successfully",
                uploaded_at=upload_request.uploaded_at,
                metadata=upload_request.metadata
            )
            
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
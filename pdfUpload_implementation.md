# PDF Upload API Implementation

## API 엔드포인트 구현

### infra/api/upload_router.py
```python
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, Any
import uuid
from datetime import datetime

from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadRequest, UploadResponse

router = APIRouter(prefix="/api/v1/upload", tags=["upload"])

@router.post("/pdf", response_model=UploadResponse)
async def upload_pdf(
    file: UploadFile = File(...),
    metadata: Dict[str, Any] = None
) -> UploadResponse:
    """
    PDF 파일 업로드 엔드포인트
    
    - 파일 검증
    - 이벤트 발행 (document.uploaded)
    - 업로드 이력 저장
    """
    # 파일 타입 검증
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")
    
    # 파일 크기 검증 (예: 50MB 제한)
    if file.size > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
    
    try:
        # 업로드 요청 생성
        upload_request = UploadRequest(
            document_id=str(uuid.uuid4()),
            filename=file.filename,
            content_type="application/pdf",
            file_size=file.size,
            metadata=metadata or {},
            uploaded_at=datetime.utcnow()
        )
        
        # 파일 내용 읽기
        file_content = await file.read()
        
        # Orchestrator를 통해 처리
        orchestrator = UploadOrchestrator()
        result = await orchestrator.process_upload(
            upload_request=upload_request,
            file_content=file_content
        )
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.post("/markdown", response_model=UploadResponse)
async def upload_markdown(
    file: UploadFile = File(...),
    metadata: Dict[str, Any] = None
) -> UploadResponse:
    """마크다운 파일 업로드 엔드포인트"""
    # Similar implementation for markdown files
    pass

@router.get("/status/{document_id}")
async def get_upload_status(document_id: str) -> Dict[str, Any]:
    """업로드 상태 조회"""
    orchestrator = UploadOrchestrator()
    status = await orchestrator.get_processing_status(document_id)
    return status
```

## Upload Service 구현

### upload_service/schema.py
```python
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import datetime

class UploadRequest(BaseModel):
    """업로드 요청 스키마"""
    document_id: str
    filename: str
    content_type: str
    file_size: int
    metadata: Dict[str, Any]
    uploaded_at: datetime

class UploadResponse(BaseModel):
    """업로드 응답 스키마"""
    document_id: str
    status: str  # "uploaded", "processing", "completed", "failed"
    message: str
    uploaded_at: datetime
    metadata: Optional[Dict[str, Any]]

class DocumentUploadedEvent(BaseModel):
    """document.uploaded 이벤트 스키마"""
    document_id: str
    filename: str
    content_type: str
    file_size: int
    file_path: str  # 임시 저장 경로
    metadata: Dict[str, Any]
    uploaded_at: datetime
    event_timestamp: datetime
```

### upload_service/orchestrator.py
```python
from typing import Dict, Any
import os
import aiofiles
from datetime import datetime

from .schema import UploadRequest, UploadResponse, DocumentUploadedEvent
from .upload_validator import UploadValidator
from .upload_handler import UploadHandler
from .event_publisher import EventPublisher
from .repository import UploadRepository
from infra.core.config import settings

class UploadOrchestrator:
    """업로드 처리 오케스트레이터"""
    
    def __init__(self):
        self.validator = UploadValidator()
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
        
        1. 파일 검증
        2. 임시 파일 저장
        3. 메타데이터 저장
        4. 이벤트 발행
        5. 응답 반환
        """
        try:
            # 1. 파일 검증
            validation_result = await self.validator.validate_file(
                filename=upload_request.filename,
                content_type=upload_request.content_type,
                file_size=upload_request.file_size,
                file_content=file_content
            )
            
            if not validation_result.is_valid:
                return UploadResponse(
                    document_id=upload_request.document_id,
                    status="failed",
                    message=validation_result.error_message,
                    uploaded_at=upload_request.uploaded_at
                )
            
            # 2. 임시 파일 저장
            file_path = await self.handler.save_temp_file(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                file_content=file_content
            )
            
            # 3. 업로드 이력 저장
            await self.repository.save_upload_record(upload_request)
            
            # 4. 이벤트 발행
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
            
            # 5. 응답 반환
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
    
    async def get_processing_status(self, document_id: str) -> Dict[str, Any]:
        """문서 처리 상태 조회"""
        return await self.repository.get_upload_status(document_id)
```

### upload_service/event_publisher.py
```python
import json
from typing import Dict, Any
from aiokafka import AIOKafkaProducer

from .schema import DocumentUploadedEvent
from infra.events.event_producer import EventProducer
from infra.core.config import settings

class EventPublisher:
    """업로드 이벤트 발행자"""
    
    def __init__(self):
        self.producer = EventProducer()
    
    async def publish_document_uploaded(self, event: DocumentUploadedEvent):
        """document.uploaded 이벤트 발행"""
        await self.producer.send_event(
            topic=settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            key=event.document_id,
            value=event.dict()
        )
```

## PDF 처리 모듈의 컨슈머 구현

### content_pdf/pdf_consumer.py
```python
import json
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer

from .orchestrator import PdfOrchestrator
from .schema import PdfProcessingRequest
from infra.events.event_consumer import EventConsumer
from infra.core.config import settings

class PdfConsumer(EventConsumer):
    """PDF 문서 처리 컨슈머"""
    
    def __init__(self):
        super().__init__(
            topics=[settings.KAFKA_TOPIC_DOCUMENT_UPLOADED],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-pdf"
        )
        self.orchestrator = PdfOrchestrator()
    
    async def handle_message(self, message: Dict[str, Any]):
        """메시지 처리"""
        # content_type이 PDF인 경우만 처리
        if message.get("content_type") != "application/pdf":
            return
        
        # PDF 처리 요청 생성
        request = PdfProcessingRequest(
            document_id=message["document_id"],
            file_path=message["file_path"],
            metadata=message.get("metadata", {})
        )
        
        # Orchestrator를 통해 처리
        await self.orchestrator.process_pdf(request)
    
    async def handle_document_uploaded(self, event_data: Dict[str, Any]):
        """document.uploaded 이벤트 핸들러"""
        await self.handle_message(event_data)
```

### infra/events/event_consumer.py
```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import json
import asyncio
from aiokafka import AIOKafkaConsumer

from infra.core.config import settings

class EventConsumer(ABC):
    """이벤트 컨슈머 베이스 클래스"""
    
    def __init__(self, topics: List[str], group_id: str):
        self.topics = topics
        self.group_id = group_id
        self.consumer = None
        self._running = False
    
    async def start(self):
        """컨슈머 시작"""
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await self.consumer.start()
        self._running = True
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                    
                await self.handle_message(msg.value)
                
        finally:
            await self.consumer.stop()
    
    async def stop(self):
        """컨슈머 중지"""
        self._running = False
    
    @abstractmethod
    async def handle_message(self, message: Dict[str, Any]):
        """메시지 처리 - 서브클래스에서 구현"""
        pass
```
# schema.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum

class ProcessingStatus(str, Enum):
    """처리 상태 enum"""
    UPLOADED = "uploaded"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class DocumentEventType(str, Enum):
    """문서 이벤트 타입 enum - 파일 타입과 content-type 통합"""
    PDF = "pdf_type"
    MARKDOWN = "markdown_type"
    JSON = "json_type"
    EMAIL = "email_type"
    
    def get_content_type(self) -> str:
        """이벤트 타입에 대응하는 content-type 반환"""
        mapping = {
            self.PDF: "application/pdf",
            self.MARKDOWN: "text/markdown",
            self.JSON: "application/json",
            self.EMAIL: "email/rfc822"
        }
        return mapping[self]

# === 모듈 간 공유 기본 스키마 ===
class ContentEvent(BaseModel):
    """이벤트 기본 스키마"""
    event_type: str
    content_id: str
    content_data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)
    timestamp: datetime

class ProcessedContent(BaseModel):
    """처리된 콘텐츠 기본 스키마"""
    content_id: str
    original_type: str
    processed_data: Any
    embeddings: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    processed_at: datetime

class ChunkData(BaseModel):
    """청크 데이터 스키마"""
    chunk_id: str
    content_id: str
    chunk_text: str
    chunk_index: int
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)

class EmbeddingData(BaseModel):
    """임베딩 데이터 스키마"""
    embedding_id: str
    content_id: str
    chunk_id: Optional[str] = None
    embedding_vector: List[float]
    embedding_text: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

# === 이벤트 스키마 (모듈 간 통신) ===
class DocumentUploadedEvent(BaseModel):
    """document.uploaded 이벤트 스키마 - 이벤트 타입 추가"""
    event_type: DocumentEventType  # pdf_type, markdown_type, json_type, email_type
    document_id: str
    filename: str
    content_type: str
    file_size: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    uploaded_at: datetime
    event_timestamp: datetime
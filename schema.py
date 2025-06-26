
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

class ContentType(str, Enum):
    """콘텐츠 타입 enum"""
    PDF = "application/pdf"
    MARKDOWN = "text/markdown"
    JSON = "application/json"
    EMAIL = "email/rfc822"

# === 기본 스키마 ===
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

# === Upload 관련 스키마 ===
class UploadRequest(BaseModel):
    """업로드 요청 스키마"""
    document_id: str
    filename: str
    content_type: str
    file_size: int
    metadata: Dict[str, Any] = Field(default_factory=dict)
    uploaded_at: datetime

class UploadResponse(BaseModel):
    """업로드 응답 스키마"""
    document_id: str
    status: ProcessingStatus
    message: str
    uploaded_at: datetime
    metadata: Optional[Dict[str, Any]] = None

class DocumentUploadedEvent(BaseModel):
    """document.uploaded 이벤트 스키마"""
    document_id: str
    filename: str
    content_type: str
    file_size: int
    file_path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    uploaded_at: datetime
    event_timestamp: datetime

# === PDF 전용 스키마 ===
class PdfProcessingRequest(BaseModel):
    """PDF 처리 요청 스키마"""
    document_id: str
    file_path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
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

# 기본 스키마들...

# PDF 전용 스키마 추가
class PdfProcessingRequest(BaseModel):
    """PDF 처리 요청 스키마"""
    document_id: str
    file_path: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
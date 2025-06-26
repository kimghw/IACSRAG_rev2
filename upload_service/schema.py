# upload_service/schema.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime
from schema import ProcessingStatus  # 루트에서 import

# === Upload 모듈 내부 스키마 ===
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

class ValidationResult(BaseModel):
    """파일 검증 결과"""
    is_valid: bool
    error_message: Optional[str] = None
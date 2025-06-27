# upload_service/schema.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
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

# === 배치 업로드 관련 스키마 (단순화) ===
class BatchUploadResult(BaseModel):
    """개별 파일 업로드 결과"""
    document_id: str
    filename: str
    status: str
    content_type: Optional[str] = None
    error: Optional[str] = None

class BatchUploadResponse(BaseModel):
    """배치 업로드 응답 - 단순히 결과 집계만"""
    total_files: int
    successful_count: int
    failed_count: int
    successful_uploads: List[BatchUploadResult]
    failed_uploads: List[BatchUploadResult]
    uploaded_at: datetime
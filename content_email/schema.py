# content_email/schema.py
from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from datetime import datetime

class EmailAttachmentInfo(BaseModel):
    """첨부파일 정보"""
    attachment_id: str
    name: str
    content_type: str
    size: int
    file_path: str
    is_downloaded: bool = False
    download_error: Optional[str] = None

class EmailProcessingRequest(BaseModel):
    """이메일 처리 요청"""
    event_id: str
    account_id: str
    event_data: Dict[str, Any]

class BatchEmailProcessingRequest(BaseModel):
    """배치 이메일 처리 요청"""
    batch_id: str
    events: List[Dict[str, Any]]

class EmailInfo(BaseModel):
    """처리된 이메일 정보"""
    email_id: str
    document_id: str
    subject: str
    sender: str
    received_datetime: str
    body_preview: str  # 본문 미리보기 (200자)
    embedding_id: Optional[str] = None
    is_read: bool
    has_attachments: bool
    attachments: List[EmailAttachmentInfo] = Field(default_factory=list)
    importance: str

class EmailProcessingResult(BaseModel):
    """이메일 처리 결과"""
    event_id: str
    account_id: str
    email_count: int
    processed_emails: List[EmailInfo]
    total_attachments: int = 0
    downloaded_attachments: int = 0
    processing_time: float
    status: str = "completed"

class BatchEmailProcessingResult(BaseModel):
    """배치 이메일 처리 결과"""
    batch_id: str
    total_events: int
    processed_events: int
    total_emails: int
    processed_emails: int
    total_attachments: int = 0
    downloaded_attachments: int = 0
    processing_time: float
    failed_events: List[str] = Field(default_factory=list)
    status: str = "completed"
# content_pdf/schema.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

class PdfProcessingRequest(BaseModel):
    """PDF 처리 요청 스키마"""
    document_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ChunkDocument(BaseModel):
    """MongoDB에 저장될 청크 문서 스키마"""
    document_id: str        # 원본 문서 ID
    chunk_id: str          # 청크 고유 ID (_id로 사용)
    chunk_index: int       # 청크 순서
    text_content: str      # 청크 텍스트 내용
    char_start: int        # 원본 텍스트에서 시작 위치
    char_end: int          # 원본 텍스트에서 끝 위치
    token_count: Optional[int] = None  # 토큰 수 (토큰 청킹 시)
    chunk_metadata: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime
    indexed_at: Optional[datetime] = None
    embedding_id: Optional[str] = None  # Qdrant 포인트 ID 참조
    
    class Config:
        # MongoDB와의 호환성을 위한 설정
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
# content_pdf/schema.py
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from datetime import datetime

# === PDF 모듈 내부 스키마 ===
class PdfProcessingRequest(BaseModel):
    """PDF 처리 요청 스키마"""
    document_id: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ChunkDocument(BaseModel):
    """MongoDB에 저장될 청크 문서 스키마"""
    document_id: str  # 문서번호
    chunk_id: str     # 청크번호
    chunk_data: Dict[str, Any]  # 청크데이터 (텍스트, 메타데이터 등)
    chunk_index: int
    created_at: datetime
    indexed_at: Optional[datetime] = None
    index_info: Optional[Dict[str, Any]] = None  # 인덱싱 정보

class PdfTextExtractedEvent(BaseModel):
    """text.extracted 이벤트 스키마 (향후 사용)"""
    document_id: str
    text_length: int
    page_count: int
    extracted_at: datetime

class PdfChunksCreatedEvent(BaseModel):
    """chunks.created 이벤트 스키마 (향후 사용)"""
    document_id: str
    chunk_count: int
    created_at: datetime
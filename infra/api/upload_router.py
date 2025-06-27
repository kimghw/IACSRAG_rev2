# infra/api/upload_router.py
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Dict, Any, Optional
import uuid
from datetime import datetime, timezone
from pathlib import Path

from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadRequest, UploadResponse
from schema import DocumentEventType

router = APIRouter(prefix="/api/v1/upload", tags=["upload"])

# 지원하는 파일 확장자와 DocumentEventType 매핑
FILE_TYPE_MAPPING = {
    '.pdf': DocumentEventType.PDF,
    '.md': DocumentEventType.MARKDOWN,
    '.markdown': DocumentEventType.MARKDOWN,
    '.json': DocumentEventType.JSON
}

def validate_file_type(filename: str) -> str:
    """파일 타입 검증 및 content-type 반환"""
    suffix = Path(filename).suffix.lower()
    
    if suffix not in FILE_TYPE_MAPPING:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type: {suffix}. Supported types: {list(FILE_TYPE_MAPPING.keys())}"
        )
    
    return FILE_TYPE_MAPPING[suffix].get_content_type()

@router.post("/file", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """범용 파일 업로드 엔드포인트 - PDF, Markdown, JSON 지원"""
    
    # 파일 타입 검증
    content_type = validate_file_type(file.filename)
    
    # 파일 크기 검증 - 50MB 제한
    if file.size and file.size > 50 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
    
    try:
        # 파일 내용 읽기
        file_content = await file.read()
        
        # 읽은 후 실제 크기 재검증
        actual_size = len(file_content)
        if actual_size > 50 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
        
        # 업로드 요청 생성
        upload_request = UploadRequest(
            document_id=str(uuid.uuid4()),
            filename=file.filename,
            content_type=content_type,  # 이미 string
            file_size=actual_size,
            metadata=metadata or {},
            uploaded_at=datetime.now(timezone.utc)
        )
        
        # 업로드 처리 (이벤트 발행 포함)
        upload_orchestrator = UploadOrchestrator()
        upload_result = await upload_orchestrator.process_upload(upload_request, file_content)
        
        return upload_result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

@router.post("/pdf", response_model=UploadResponse)
async def upload_pdf(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """PDF 전용 업로드 엔드포인트"""
    
    # PDF 검증
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")
    
    return await upload_file(file, metadata)

@router.post("/markdown", response_model=UploadResponse)
async def upload_markdown(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """Markdown 전용 업로드 엔드포인트"""
    
    # Markdown 검증
    if not any(file.filename.lower().endswith(ext) for ext in ['.md', '.markdown']):
        raise HTTPException(status_code=400, detail="Only Markdown files are allowed")
    
    return await upload_file(file, metadata)

@router.post("/json", response_model=UploadResponse)
async def upload_json(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """JSON 전용 업로드 엔드포인트"""
    
    # JSON 검증
    if not file.filename.lower().endswith('.json'):
        raise HTTPException(status_code=400, detail="Only JSON files are allowed")
    
    return await upload_file(file, metadata)

@router.get("/supported-types")
async def get_supported_types():
    """지원하는 파일 타입 조회"""
    return {
        "supported_types": [
            {
                "extension": ext,
                "event_type": event_type.value,
                "content_type": event_type.get_content_type(),
                "endpoint": f"/api/v1/upload/{ext[1:]}" if ext != '.markdown' else "/api/v1/upload/markdown"
            }
            for ext, event_type in FILE_TYPE_MAPPING.items()
        ],
        "generic_endpoint": "/api/v1/upload/file",
        "max_file_size": "50MB"
    }
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Dict, Any, Optional
import uuid
from datetime import datetime, timezone

from upload_service.orchestrator import UploadOrchestrator
from schema import UploadRequest, UploadResponse

router = APIRouter(prefix="/api/v1/upload", tags=["upload"])

@router.post("/pdf", response_model=UploadResponse)
async def upload_pdf(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """PDF 파일 업로드 엔드포인트"""
    
    # 기본 검증
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="Only PDF files are allowed")
    
    if file.size > 50 * 1024 * 1024:  # 50MB 제한
        raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
    
    try:
        # 업로드 요청 생성
        upload_request = UploadRequest(
            document_id=str(uuid.uuid4()),
            filename=file.filename,
            content_type="application/pdf",
            file_size=file.size,
            metadata=metadata or {},
            uploaded_at=datetime.now(timezone.utc)
        )
        
        # 파일 내용 읽기
        file_content = await file.read()
        
        # Orchestrator를 통해 처리
        orchestrator = UploadOrchestrator()
        result = await orchestrator.process_upload(upload_request, file_content)
        
        return result
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")

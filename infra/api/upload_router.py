from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import Dict, Any, Optional
import uuid
from datetime import datetime, timezone

from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadRequest, UploadResponse

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
    
    # 파일 크기 검증 - content-length 헤더 사용
    if file.size and file.size > 50 * 1024 * 1024:  # 50MB 제한
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
            content_type="application/pdf",
            file_size=actual_size,  # 실제 읽은 크기 사용
            metadata=metadata or {},
            uploaded_at=datetime.now(timezone.utc)
        )
        
        # Orchestrator를 통해 처리
        orchestrator = UploadOrchestrator()
        result = await orchestrator.process_upload(upload_request, file_content)
        
        return result
        
    except HTTPException:
        # HTTPException은 그대로 전달
        raise
    except Exception as e:
        # 기타 예외는 500 에러로 변환
        raise HTTPException(status_code=500, detail=f"Upload failed: {str(e)}")
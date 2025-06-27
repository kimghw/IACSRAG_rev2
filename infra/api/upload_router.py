# infra/api/upload_router.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from typing import Dict, Any, Optional, List
import uuid
from datetime import datetime, timezone
from pathlib import Path
import asyncio
import json

from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadRequest, UploadResponse, BatchUploadResponse, BatchUploadResult
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

@router.post("/batch", response_model=BatchUploadResponse)
async def upload_batch(
    files: List[UploadFile] = File(...),
    metadata: Optional[str] = Form(None)  # JSON string로 받음
) -> BatchUploadResponse:
    """
    배치 파일 업로드 엔드포인트
    - 여러 파일을 동시에 업로드
    - 각 파일은 독립적으로 처리됨
    - 파일별로 개별 이벤트 발행
    """
    
    # 메타데이터 파싱
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid metadata JSON format")
    
    # 업로드 orchestrator
    upload_orchestrator = UploadOrchestrator()
    
    # 각 파일에 대한 처리 태스크 생성
    upload_tasks = []
    
    for file in files:
        # 파일 타입 검증
        try:
            content_type = validate_file_type(file.filename)
        except HTTPException:
            # 지원하지 않는 파일 타입은 스킵
            continue
        
        # 파일별 독립적인 처리
        async def process_single_file(f: UploadFile) -> Dict[str, Any]:
            """단일 파일 처리"""
            try:
                # 파일 내용 읽기
                file_content = await f.read()
                
                # 파일 크기 확인
                if len(file_content) > 50 * 1024 * 1024:
                    raise ValueError(f"File size exceeds 50MB limit: {f.filename}")
                
                # 각 파일별 독립적인 document_id
                document_id = str(uuid.uuid4())
                
                # UploadRequest 생성
                upload_request = UploadRequest(
                    document_id=document_id,
                    filename=f.filename,
                    content_type=validate_file_type(f.filename),
                    file_size=len(file_content),
                    metadata=parsed_metadata.copy(),  # 공통 메타데이터만 사용
                    uploaded_at=datetime.now(timezone.utc)
                )
                
                # 업로드 처리 (DB 저장 + 이벤트 발행)
                result = await upload_orchestrator.process_upload(upload_request, file_content)
                
                return {
                    "success": True,
                    "document_id": result.document_id,
                    "filename": f.filename,
                    "status": result.status,
                    "content_type": upload_request.content_type
                }
                
            except Exception as e:
                return {
                    "success": False,
                    "filename": f.filename,
                    "error": str(e)
                }
        
        # 태스크 추가
        task = process_single_file(file)
        upload_tasks.append(task)
    
    # 모든 업로드 태스크 동시 실행
    results = await asyncio.gather(*upload_tasks)
    
    # 결과 정리
    successful_uploads = []
    failed_uploads = []
    
    for result in results:
        if result["success"]:
            successful_uploads.append(BatchUploadResult(
                document_id=result["document_id"],
                filename=result["filename"],
                status=result["status"],
                content_type=result.get("content_type")
            ))
        else:
            failed_uploads.append(BatchUploadResult(
                document_id="",
                filename=result["filename"],
                status="failed",
                error=result.get("error", "Unknown error")
            ))
    
    # 배치 업로드 응답 생성
    return BatchUploadResponse(
        total_files=len(files),
        successful_count=len(successful_uploads),
        failed_count=len(failed_uploads),
        successful_uploads=successful_uploads,
        failed_uploads=failed_uploads,
        uploaded_at=datetime.now(timezone.utc)
    )

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
            content_type=content_type,
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
        "batch_endpoint": "/api/v1/upload/batch",
        "max_file_size": "50MB",
        "batch_support": True
    }
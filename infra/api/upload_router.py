

# infra/api/upload_router.py
from fastapi import APIRouter, UploadFile, File, HTTPException, Form
from typing import Dict, Any, Optional, List
import json

from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadResponse, BatchUploadResponse, FileUploadData

router = APIRouter(prefix="/api/v1/upload", tags=["upload"])

@router.post("/batch", response_model=BatchUploadResponse)
async def upload_batch(
    files: List[UploadFile] = File(...),
    metadata: Optional[str] = Form(None)  # JSON string로 받음
) -> BatchUploadResponse:
    """
    배치 파일 업로드 엔드포인트
    - 여러 파일을 동시에 업로드
    - 각 파일은 독립적으로 처리됨
    - 중복 파일은 기존 document_id 반환
    """
    # 메타데이터 파싱
    parsed_metadata = {}
    if metadata:
        try:
            parsed_metadata = json.loads(metadata)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid metadata JSON format")
    
    # 파일 데이터 준비
    file_data_list = []
    for file in files:
        content = await file.read()
        file_data_list.append(FileUploadData(
            filename=file.filename,
            content=content
        ))
    
    # Orchestrator에 위임
    upload_orchestrator = UploadOrchestrator()
    return await upload_orchestrator.process_batch_upload(
        files=file_data_list,
        metadata=parsed_metadata
    )

@router.post("/file", response_model=UploadResponse)
async def upload_file(
    file: UploadFile = File(...),
    metadata: Optional[Dict[str, Any]] = None
) -> UploadResponse:
    """범용 파일 업로드 엔드포인트 - PDF, Markdown, JSON 지원"""
    # 파일 내용 읽기
    file_content = await file.read()
    
    # Orchestrator에 위임
    upload_orchestrator = UploadOrchestrator()
    return await upload_orchestrator.process_single_upload(
        filename=file.filename,
        file_content=file_content,
        metadata=metadata
    )

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
    upload_orchestrator = UploadOrchestrator()
    return upload_orchestrator.get_supported_types()
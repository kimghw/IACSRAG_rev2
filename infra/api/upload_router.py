# infra/api/upload_router.py

from fastapi import APIRouter, UploadFile, File, Depends
from upload_service.orchestrator import UploadServiceOrchestrator

router = APIRouter()

@router.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Handles file uploads.
    """
    orchestrator = UploadServiceOrchestrator()
    result = await orchestrator.handle_upload(file)
    return result

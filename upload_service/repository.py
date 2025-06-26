# upload_service/repository.py
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from infra.databases.mongo_db import MongoDB
from schema import UploadRequest

logger = logging.getLogger(__name__)

class UploadRepository:
    """업로드 데이터 접근 계층"""
    
    def __init__(self):
        self.mongo = MongoDB()
    
    async def save_upload_record(self, upload_request: UploadRequest):
        """업로드 이력 저장"""
        document = {
            "document_id": upload_request.document_id,
            "filename": upload_request.filename,
            "content_type": upload_request.content_type,
            "file_size": upload_request.file_size,
            "metadata": upload_request.metadata,
            "status": "uploaded",
            "uploaded_at": upload_request.uploaded_at,
            "created_at": datetime.utcnow()
        }
        
        await self.mongo.insert_one("uploads", document)
        logger.info(f"Upload record saved for document: {upload_request.document_id}")
    
    async def update_upload_status(
        self, 
        document_id: str, 
        status: str, 
        error_message: Optional[str] = None
    ):
        """업로드 상태 업데이트"""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if error_message:
            update_data["error_message"] = error_message
        
        result = await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
        
        if result:
            logger.info(f"Upload status updated for document: {document_id} to {status}")
        else:
            logger.warning(f"No document found with id: {document_id}")
    
    async def get_upload_status(self, document_id: str) -> Optional[Dict[str, Any]]:
        """업로드 상태 조회"""
        document = await self.mongo.find_one(
            collection="uploads",
            filter={"document_id": document_id}
        )
        
        if document:
            # MongoDB ObjectId 제거
            document.pop("_id", None)
            return document
        
        return None
# upload_service/repository.py
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
import asyncio
from infra.databases.mongo_db import MongoDB
from .schema import UploadRequest
import logging

logger = logging.getLogger(__name__)

class UploadRepository:
    def __init__(self):
        self.mongo = MongoDB()
    
    async def save_upload_with_file(self, upload_request: UploadRequest, file_content: bytes, quick_hash: Optional[str] = None):
        """
        파일 내용과 메타데이터 저장
        1. GridFS에 파일 저장
        2. uploads 컬렉션에 메타데이터 저장 (해시 포함)
        """
        # GridFS로 파일 저장
        fs = AsyncIOMotorGridFSBucket(self.mongo.db)
        file_id = await fs.upload_from_stream(
            upload_request.filename,
            file_content,
            metadata={
                "document_id": upload_request.document_id,
                "content_type": upload_request.content_type
            }
        )
        
        # 메타데이터 저장
        document = {
            "document_id": upload_request.document_id,
            "filename": upload_request.filename,
            "gridfs_file_id": str(file_id),
            "content_type": upload_request.content_type,
            "file_size": upload_request.file_size,
            "metadata": upload_request.metadata,
            "status": "uploaded",
            "uploaded_at": upload_request.uploaded_at,
            "created_at": datetime.now(timezone.utc)
        }
        
        # 해시가 있으면 추가
        if quick_hash:
            document["quick_hash"] = quick_hash
        
        await self.mongo.insert_one("uploads", document)
        if quick_hash:
            logger.debug(f"Saved upload metadata for document: {upload_request.document_id} with hash: {quick_hash[:16]}...")
        else:
            logger.debug(f"Saved upload metadata for document: {upload_request.document_id} (no hash)")
    
    async def update_upload_status(self, document_id: str, status: str, error_message: str = None):
        """업로드 상태 업데이트"""
        update_data = {
            "status": status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if error_message:
            update_data["error_message"] = error_message
        
        await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
    
    async def find_by_hash(self, quick_hash: str) -> Optional[Dict[str, Any]]:
        """quick_hash로 문서 조회 (중복 검사용)"""
        return await self.mongo.find_one("uploads", {"quick_hash": quick_hash})
    
    async def find_by_quick_hash(self, quick_hash: str) -> Optional[Dict[str, Any]]:
        """quick_hash로 문서 조회 (중복 검사용) - 별칭 메서드"""
        return await self.find_by_hash(quick_hash)
    
    async def get_document_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """document_id로 문서 조회"""
        return await self.mongo.find_one("uploads", {"document_id": document_id})
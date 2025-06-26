# upload_service/repository.py
from typing import Dict, Any, Optional
from datetime import datetime
import logging

from infra.databases.mongo_db import MongoDB
from schema import UploadRequest

logger = logging.getLogger(__name__)

# upload_service/repository.py
class UploadRepository:
    async def save_upload_with_file(self, upload_request: UploadRequest, file_content: bytes):
        """파일 내용과 함께 저장 (GridFS 사용)"""
        # GridFS로 저장
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
            "uploaded_at": upload_request.uploaded_at
        }
        
        await self.mongo.insert_one("uploads", document)
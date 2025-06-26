# upload_service/repository.py
from datetime import datetime, timezone
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from infra.databases.mongo_db import MongoDB
from schema import UploadRequest

class UploadRepository:
    def __init__(self):
        self.mongo = MongoDB()
    
    async def save_upload_with_file(self, upload_request: UploadRequest, file_content: bytes):
        """파일 내용과 함께 저장 (GridFS 사용)"""
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
            "uploaded_at": upload_request.uploaded_at
        }
        
        await self.mongo.insert_one("uploads", document)
    
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
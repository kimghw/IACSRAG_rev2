# upload_service/orchestrator.py
import time
import asyncio
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any, Optional
from fastapi import HTTPException

from .repository import UploadRepository
from .event_publisher import EventPublisher
from .file_hasher import FileHasher
from .schema import (
    UploadRequest, UploadResponse, BatchUploadResponse, 
    BatchUploadResult, FileUploadData
)
from schema import ProcessingStatus, DocumentEventType, DocumentUploadedEvent
from infra.core.processing_logger import processing_logger
from infra.core.config import settings
import logging

logger = logging.getLogger(__name__)

class UploadOrchestrator:
    def __init__(self):
        self.repository = UploadRepository()
        self.event_publisher = EventPublisher()
        self.file_hasher = FileHasher()
        # 환경 설정에서 가져오기
        self.max_concurrent_uploads = settings.UPLOAD_MAX_CONCURRENT
        self.upload_semaphore = asyncio.Semaphore(self.max_concurrent_uploads)
        
        logger.info(f"Upload orchestrator initialized with max concurrent uploads: {self.max_concurrent_uploads}")
        
        # 지원하는 파일 확장자와 DocumentEventType 매핑
        self.FILE_TYPE_MAPPING = {
            '.pdf': DocumentEventType.PDF,
            '.md': DocumentEventType.MARKDOWN,
            '.markdown': DocumentEventType.MARKDOWN,
            '.json': DocumentEventType.JSON
        }
    
    def validate_file_type(self, filename: str) -> str:
        """파일 타입 검증 및 content-type 반환"""
        suffix = Path(filename).suffix.lower()
        
        if suffix not in self.FILE_TYPE_MAPPING:
            raise ValueError(f"Unsupported file type: {suffix}. Supported types: {list(self.FILE_TYPE_MAPPING.keys())}")
        
        return self.FILE_TYPE_MAPPING[suffix].get_content_type()
    
    def _determine_event_type(self, filename: str) -> DocumentEventType:
        """파일 확장자를 기반으로 이벤트 타입 결정"""
        suffix = Path(filename).suffix.lower()
        
        if suffix not in self.FILE_TYPE_MAPPING:
            raise ValueError(f"Unsupported file type: {suffix}")
            
        return self.FILE_TYPE_MAPPING[suffix]
    
    async def _check_duplicate(self, quick_hash: str, filename: str) -> Optional[Dict[str, Any]]:
        """
        파일 중복 검사
        Returns:
            중복된 파일 정보 또는 None
        """
        existing_file = await self.repository.find_by_quick_hash(quick_hash)
        
        if existing_file:
            logger.info(f"Duplicate file detected: {filename} (hash: {quick_hash[:16]}...)")
            logger.info(f"  - Existing document_id: {existing_file['document_id']}")
            logger.info(f"  - Uploaded at: {existing_file['uploaded_at']}")
            return existing_file
        
        return None
    
    async def process_single_upload(
        self, 
        filename: str, 
        file_content: bytes, 
        metadata: Optional[Dict[str, Any]] = None
    ) -> UploadResponse:
        """
        단일 파일 업로드 처리
        Router에서 직접 호출하는 메인 메서드
        """
        # 파일 타입 검증
        try:
            content_type = self.validate_file_type(filename)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        
        # 파일 크기 검증 - 50MB 제한
        if len(file_content) > 50 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="File size exceeds 50MB limit")
        
        # UploadRequest 생성
        upload_request = UploadRequest(
            document_id=str(uuid.uuid4()),
            filename=filename,
            content_type=content_type,
            file_size=len(file_content),
            metadata=metadata or {},
            uploaded_at=datetime.now(timezone.utc)
        )
        
        # 실제 처리
        return await self._process_upload(upload_request, file_content)
    
    async def process_batch_upload(
        self,
        files: List[FileUploadData],
        metadata: Optional[Dict[str, Any]] = None
    ) -> BatchUploadResponse:
        """
        배치 파일 업로드 처리
        Router에서 파일 데이터를 전달받아 처리
        """
        upload_tasks = []
        
        for file_data in files:
            # 각 파일을 독립적으로 처리
            async def process_file(f: FileUploadData) -> Dict[str, Any]:
                try:
                    result = await self.process_single_upload(
                        filename=f.filename,
                        file_content=f.content,
                        metadata=metadata
                    )
                    
                    return {
                        "success": True,
                        "document_id": result.document_id,
                        "filename": f.filename,
                        "status": result.status,
                        "content_type": result.metadata.get("content_type", ""),
                        "is_duplicate": result.status == ProcessingStatus.DUPLICATE,
                        "message": result.message
                    }
                    
                except HTTPException as e:
                    return {
                        "success": False,
                        "filename": f.filename,
                        "error": e.detail
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "filename": f.filename,
                        "error": str(e)
                    }
            
            upload_tasks.append(process_file(file_data))
        
        # 모든 업로드 태스크 동시 실행
        results = await asyncio.gather(*upload_tasks)
        
        # 결과 정리
        successful_uploads = []
        failed_uploads = []
        duplicate_count = 0
        
        for result in results:
            if result["success"]:
                if result.get("is_duplicate", False):
                    duplicate_count += 1
                
                successful_uploads.append(BatchUploadResult(
                    document_id=result["document_id"],
                    filename=result["filename"],
                    status=result["status"],
                    content_type=result.get("content_type"),
                    error=result.get("message") if result.get("is_duplicate") else None
                ))
            else:
                failed_uploads.append(BatchUploadResult(
                    document_id="",
                    filename=result["filename"],
                    status="failed",
                    error=result.get("error", "Unknown error")
                ))
        
        if duplicate_count > 0:
            logger.info(f"Batch upload completed: {duplicate_count} duplicate files detected")
        
        return BatchUploadResponse(
            total_files=len(files),
            successful_count=len(successful_uploads),
            failed_count=len(failed_uploads),
            successful_uploads=successful_uploads,
            failed_uploads=failed_uploads,
            uploaded_at=datetime.now(timezone.utc)
        )
    
    async def _process_upload(self, upload_request: UploadRequest, file_content: bytes) -> UploadResponse:
        """
        내부 업로드 처리 로직
        1. 파일 중복 검사 (quick hash) - 설정에 따라
        2. MongoDB에 파일 저장 (GridFS)
        3. 메타데이터 저장
        4. 파일 타입에 맞는 이벤트 발행
        """
        async with self.upload_semaphore:  # 동시 업로드 수 제한
            start_time = time.time()
            
            # 업로드 시작 로그
            processing_logger.upload_started(
                document_id=upload_request.document_id,
                filename=upload_request.filename,
                file_size=upload_request.file_size
            )
            
            try:
                quick_hash = None
                existing_file = None
                
                # 설정에 따라 해시 생성 및 중복 검사
                if settings.UPLOAD_APPLY_HASHER:
                    logger.info(f"Hash generation enabled (UPLOAD_APPLY_HASHER={settings.UPLOAD_APPLY_HASHER})")
                    
                    # 1. 파일 hash 생성
                    quick_hash = self.file_hasher.generate_quick_hash(
                        filename=upload_request.filename,
                        file_content=file_content
                    )
                    logger.info(f"Generated quick hash for {upload_request.filename}: {quick_hash[:16]}...")
                    
                    # 2. 중복 검사
                    existing_file = await self._check_duplicate(quick_hash, upload_request.filename)
                    
                    if existing_file:
                        # 중복 파일인 경우
                        duration = time.time() - start_time
                        processing_logger.upload_completed(
                            document_id=existing_file['document_id'],
                            duration=duration
                        )
                        
                        # 기존 파일의 document_id 반환
                        return UploadResponse(
                            document_id=existing_file['document_id'],
                            status=ProcessingStatus.UPLOADED,
                            message=f"File already exists. Using existing document_id: {existing_file['document_id']}",
                            uploaded_at=existing_file['uploaded_at'],
                            metadata=existing_file.get('metadata', {})
                        )
                else:
                    logger.info(f"Hash generation disabled (UPLOAD_APPLY_HASHER={settings.UPLOAD_APPLY_HASHER})")
                
                # 3. 새 파일인 경우 - 파일 타입에 따른 이벤트 타입 결정
                event_type = self._determine_event_type(upload_request.filename)
                logger.info(f"Determined event type: {event_type.value} for file: {upload_request.filename}")
                
                # content_type을 metadata에 저장 (응답시 필요)
                upload_request.metadata["content_type"] = upload_request.content_type
                
                # 4. MongoDB에 파일과 메타데이터 저장 (quick_hash 포함)
                await self.repository.save_upload_with_file(
                    upload_request=upload_request, 
                    file_content=file_content,
                    quick_hash=quick_hash  # None일 수도 있음
                )
                
                # 5. 이벤트 발행 - 파일 타입별로 구분
                event = DocumentUploadedEvent(
                    event_type=event_type,
                    document_id=upload_request.document_id,
                    filename=upload_request.filename,
                    content_type=upload_request.content_type,
                    file_size=upload_request.file_size,
                    metadata=upload_request.metadata,
                    uploaded_at=upload_request.uploaded_at,
                    event_timestamp=datetime.now(timezone.utc)
                )
                
                await self.event_publisher.publish_document_uploaded(event)
                logger.info(f"Published {event_type.value} event for document: {upload_request.document_id}")
                
                # 업로드 완료 로그
                duration = time.time() - start_time
                processing_logger.upload_completed(
                    document_id=upload_request.document_id,
                    duration=duration
                )
                
                return UploadResponse(
                    document_id=upload_request.document_id,
                    status=ProcessingStatus.UPLOADED,
                    message=f"File uploaded successfully with event type: {event_type.value}",
                    uploaded_at=upload_request.uploaded_at,
                    metadata=upload_request.metadata
                )
                
            except ValueError as e:
                # 지원하지 않는 파일 타입
                processing_logger.error(
                    document_id=upload_request.document_id,
                    step="upload",
                    error=str(e)
                )
                raise
                
            except Exception as e:
                processing_logger.error(
                    document_id=upload_request.document_id,
                    step="upload",
                    error=str(e)
                )
                await self.repository.update_upload_status(
                    document_id=upload_request.document_id,
                    status="failed",
                    error_message=str(e)
                )
                raise
    
    def get_supported_types(self) -> Dict[str, Any]:
        """지원하는 파일 타입 정보 반환"""
        return {
            "supported_types": [
                {
                    "extension": ext,
                    "event_type": event_type.value,
                    "content_type": event_type.get_content_type(),
                    "endpoint": f"/api/v1/upload/{ext[1:]}" if ext != '.markdown' else "/api/v1/upload/markdown"
                }
                for ext, event_type in self.FILE_TYPE_MAPPING.items()
            ],
            "generic_endpoint": "/api/v1/upload/file",
            "batch_endpoint": "/api/v1/upload/batch",
            "max_file_size": "50MB",
            "batch_support": True
        }
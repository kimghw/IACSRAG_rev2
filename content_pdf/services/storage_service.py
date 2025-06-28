# content_pdf/services/storage_service.py
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import logging
import asyncio
from motor.motor_asyncio import AsyncIOMotorClientSession

from infra.databases.mongo_db import MongoDB
from infra.databases.qdrant_db import QdrantDB
from ..schema import ChunkDocument
from schema import EmbeddingData

logger = logging.getLogger(__name__)

class StorageService:
    """통합 저장 서비스 - 트랜잭션 처리 개선"""
    
    def __init__(self):
        self.mongo = MongoDB()
        self.qdrant = QdrantDB()
        self.chunk_collection = "pdf_chunks"
        self.failed_embeddings_collection = "qdrant_failures"
    
    async def save_chunks(self, chunks: List[ChunkDocument]) -> List[str]:
        """청크들을 MongoDB에 저장"""
        documents = []
        chunk_ids = []
        
        for chunk in chunks:
            doc = {
                "_id": chunk.chunk_id,
                "document_id": chunk.document_id,
                "chunk_index": chunk.chunk_index,
                "text_content": chunk.text_content,
                "char_start": chunk.char_start,
                "char_end": chunk.char_end,
                "token_count": chunk.token_count,
                "chunk_metadata": chunk.chunk_metadata,
                "created_at": chunk.created_at,
                "indexed_at": None,
                "embedding_id": None,
                "embedding_status": "pending"  # pending, completed, failed
            }
            documents.append(doc)
            chunk_ids.append(chunk.chunk_id)
        
        if documents:
            try:
                result = await self.mongo.db[self.chunk_collection].insert_many(documents)
                logger.debug(f"Saved {len(result.inserted_ids)} chunks to MongoDB")
            except Exception as e:
                logger.error(f"Failed to save chunks to MongoDB: {e}")
                raise
        
        return chunk_ids
    
    async def save_embeddings(
        self,
        embeddings: List[EmbeddingData],
        chunks: List[ChunkDocument]
    ) -> Dict[str, Any]:
        """Qdrant에 임베딩 저장 - 실패 시 복구 로직 포함"""
        points = []
        chunk_map = {chunk.chunk_id: chunk for chunk in chunks}
        successful_ids = []
        failed_ids = []
        
        # Qdrant 포인트 준비
        for embedding in embeddings:
            chunk = chunk_map.get(embedding.chunk_id)
            if not chunk:
                continue
            
            point = {
                "id": embedding.embedding_id,
                "vector": embedding.embedding_vector,
                "payload": {
                    "mongo_db": self.mongo.db.name,
                    "mongo_collection": self.chunk_collection,
                    "mongo_chunk_id": chunk.chunk_id,
                    "document_id": chunk.document_id,
                    "chunk_index": chunk.chunk_index,
                    "text_preview": chunk.text_content[:200],
                    "content_type": "pdf",
                    **chunk.chunk_metadata,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
            }
            points.append(point)
        
        if not points:
            return {"successful": 0, "failed": 0}
        
        try:
            # Qdrant에 저장 시도
            await self.qdrant.upsert_points(points)
            successful_ids = [e.chunk_id for e in embeddings]
            logger.debug(f"Saved {len(points)} embeddings to Qdrant")
            
            # MongoDB 상태 업데이트 - 성공
            await self._update_chunk_embedding_status(
                chunk_ids=successful_ids,
                status="completed",
                embedding_map={e.chunk_id: e.embedding_id for e in embeddings}
            )
            
        except Exception as e:
            logger.error(f"Failed to save embeddings to Qdrant: {e}")
            failed_ids = [e.chunk_id for e in embeddings]
            
            # 실패 정보 저장 (나중에 재시도 가능)
            await self._save_failed_embeddings(embeddings, chunks, str(e))
            
            # MongoDB 상태 업데이트 - 실패
            await self._update_chunk_embedding_status(
                chunk_ids=failed_ids,
                status="failed",
                error_message=str(e)
            )
            
            # 에러는 전파하지 않고 로그만 남김 (배치 처리 계속)
            # raise를 하면 전체 배치가 중단됨
        
        return {
            "successful": len(successful_ids),
            "failed": len(failed_ids)
        }
    
    async def _update_chunk_embedding_status(
        self,
        chunk_ids: List[str],
        status: str,
        embedding_map: Dict[str, str] = None,
        error_message: str = None
    ):
        """청크의 임베딩 상태 업데이트"""
        update_data = {
            "embedding_status": status,
            "embedding_updated_at": datetime.now(timezone.utc)
        }
        
        if error_message:
            update_data["embedding_error"] = error_message
        
        # 개별 업데이트 (embedding_id가 다르므로)
        if embedding_map and status == "completed":
            for chunk_id, embedding_id in embedding_map.items():
                await self.mongo.db[self.chunk_collection].update_one(
                    {"_id": chunk_id},
                    {"$set": {
                        **update_data,
                        "embedding_id": embedding_id,
                        "indexed_at": datetime.now(timezone.utc)
                    }}
                )
        else:
            # 일괄 업데이트 (실패 시)
            await self.mongo.db[self.chunk_collection].update_many(
                {"_id": {"$in": chunk_ids}},
                {"$set": update_data}
            )
    
    async def _save_failed_embeddings(
        self,
        embeddings: List[EmbeddingData],
        chunks: List[ChunkDocument],
        error_message: str
    ):
        """실패한 임베딩 정보 저장 (재시도용)"""
        failed_records = []
        
        for embedding in embeddings:
            chunk = next((c for c in chunks if c.chunk_id == embedding.chunk_id), None)
            if chunk:
                failed_records.append({
                    "document_id": chunk.document_id,
                    "chunk_id": chunk.chunk_id,
                    "embedding_id": embedding.embedding_id,
                    "embedding_vector_size": len(embedding.embedding_vector),
                    "error_message": error_message,
                    "failed_at": datetime.now(timezone.utc),
                    "retry_count": 0,
                    "status": "pending_retry"
                })
        
        if failed_records:
            try:
                await self.mongo.db[self.failed_embeddings_collection].insert_many(failed_records)
                logger.info(f"Saved {len(failed_records)} failed embedding records for retry")
            except Exception as e:
                logger.error(f"Failed to save failed embedding records: {e}")
    
    async def save_with_transaction(
        self,
        chunks: List[ChunkDocument],
        embeddings: List[EmbeddingData]
    ) -> Dict[str, Any]:
        """
        트랜잭션 방식으로 저장 (선택적 사용)
        MongoDB 4.0+ 에서만 사용 가능
        """
        async with await self.mongo._client.start_session() as session:
            async with session.start_transaction():
                try:
                    # 1. MongoDB에 청크 저장
                    chunk_ids = await self.save_chunks(chunks)
                    
                    # 2. Qdrant에 임베딩 저장
                    result = await self.save_embeddings(embeddings, chunks)
                    
                    # 3. 실패가 있으면 롤백
                    if result["failed"] > 0:
                        await session.abort_transaction()
                        raise Exception(f"Failed to save {result['failed']} embeddings")
                    
                    # 4. 모두 성공하면 커밋
                    await session.commit_transaction()
                    
                    return {
                        "chunks_saved": len(chunk_ids),
                        "embeddings_saved": result["successful"],
                        "status": "completed"
                    }
                    
                except Exception as e:
                    await session.abort_transaction()
                    logger.error(f"Transaction failed: {e}")
                    raise
    
    async def retry_failed_embeddings(self, document_id: str = None):
        """실패한 임베딩 재시도"""
        filter_query = {"status": "pending_retry", "retry_count": {"$lt": 3}}
        if document_id:
            filter_query["document_id"] = document_id
        
        failed_records = await self.mongo.db[self.failed_embeddings_collection].find(
            filter_query
        ).to_list(None)
        
        if not failed_records:
            logger.info("No failed embeddings to retry")
            return
        
        logger.info(f"Retrying {len(failed_records)} failed embeddings")
        
        # 재시도 로직 구현
        # ... (구현 생략)
    
    async def update_status(
        self, 
        document_id: str, 
        status: str, 
        error_message: str = None,
        processing_info: Dict[str, Any] = None
    ):
        """문서 처리 상태 업데이트"""
        update_data = {
            "status": status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if error_message:
            update_data["error_message"] = error_message
        
        if processing_info:
            update_data["processing_info"] = processing_info
            
            # 실패한 임베딩이 있는지 확인
            storage_errors = processing_info.get("storage_errors", 0)
            embedding_errors = processing_info.get("embedding_errors", 0)
            
            if storage_errors > 0 or embedding_errors > 0:
                update_data["has_partial_failure"] = True
                update_data["partial_failure_info"] = {
                    "storage_errors": storage_errors,
                    "embedding_errors": embedding_errors,
                    "timestamp": datetime.now(timezone.utc)
                }
        
        await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
    
    async def get_processing_stats(self, document_id: str) -> Dict[str, Any]:
        """문서 처리 통계 조회"""
        # 청크 통계
        chunk_stats = await self.mongo.db[self.chunk_collection].aggregate([
            {"$match": {"document_id": document_id}},
            {"$group": {
                "_id": "$embedding_status",
                "count": {"$sum": 1}
            }}
        ]).to_list(None)
        
        stats = {
            "total_chunks": 0,
            "completed_embeddings": 0,
            "failed_embeddings": 0,
            "pending_embeddings": 0
        }
        
        for stat in chunk_stats:
            status = stat["_id"]
            count = stat["count"]
            stats["total_chunks"] += count
            
            if status == "completed":
                stats["completed_embeddings"] = count
            elif status == "failed":
                stats["failed_embeddings"] = count
            elif status == "pending":
                stats["pending_embeddings"] = count
        
        return stats
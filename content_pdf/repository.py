# content_pdf/repository.py
from motor.motor_asyncio import AsyncIOMotorClientSession
from contextlib import asynccontextmanager
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import logging

from infra.databases.qdrant_db import QdrantDB
from infra.databases.mongo_db import MongoDB
from .schema import ChunkDocument
from schema import EmbeddingData

logger = logging.getLogger(__name__)

class PdfRepository:
    """PDF 데이터 저장 관리"""
    
    def __init__(self):
        self.qdrant = QdrantDB()
        self.mongo = MongoDB()
        self.chunk_collection = "pdf_chunks"
    
    @asynccontextmanager
    async def create_transaction(self):
        """MongoDB 트랜잭션 컨텍스트 매니저"""
        async with await self.mongo.client.start_session() as session:
            async with session.start_transaction():
                try:
                    yield session
                except Exception as e:
                    await session.abort_transaction()
                    raise
    
    async def save_chunks_batch(
        self, 
        chunks: List[ChunkDocument],
        transaction: Optional[AsyncIOMotorClientSession] = None
    ) -> List[str]:
        """청크들을 MongoDB에 배치로 저장"""
        documents = []
        chunk_ids = []
        
        for chunk in chunks:
            doc = {
                "_id": chunk.chunk_id,  # _id로 chunk_id 사용
                "document_id": chunk.document_id,
                "chunk_index": chunk.chunk_index,
                "text_content": chunk.text_content,
                "char_start": chunk.char_start,
                "char_end": chunk.char_end,
                "token_count": chunk.token_count,
                "chunk_metadata": chunk.chunk_metadata,
                "created_at": chunk.created_at,
                "indexed_at": None,
                "embedding_id": None
            }
            documents.append(doc)
            chunk_ids.append(chunk.chunk_id)
        
        if documents:
            if transaction:
                result = await self.mongo.db[self.chunk_collection].insert_many(
                    documents, 
                    session=transaction
                )
            else:
                result = await self.mongo.db[self.chunk_collection].insert_many(documents)
            
            logger.debug(f"Saved {len(result.inserted_ids)} chunks to MongoDB")
        
        return chunk_ids
    
    async def save_embeddings_with_references(
        self,
        embeddings: List[EmbeddingData],
        chunks: List[ChunkDocument]
    ):
        """Qdrant에 임베딩 저장 (MongoDB 참조 정보 포함)"""
        points = []
        chunk_map = {chunk.chunk_id: chunk for chunk in chunks}
        
        for embedding in embeddings:
            chunk = chunk_map.get(embedding.chunk_id)
            if not chunk:
                continue
            
            point = {
                "id": embedding.embedding_id,
                "vector": embedding.embedding_vector,
                "payload": {
                    # MongoDB 직접 참조
                    "mongo_db": self.mongo.db.name,
                    "mongo_collection": self.chunk_collection,
                    "mongo_chunk_id": chunk.chunk_id,
                    "document_id": chunk.document_id,
                    
                    # 검색용 메타데이터
                    "chunk_index": chunk.chunk_index,
                    "text_preview": chunk.text_content[:200],
                    "content_type": "pdf",
                    
                    # 추가 정보
                    **chunk.chunk_metadata,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
            }
            points.append(point)
        
        try:
            await self.qdrant.upsert_points(points)
            logger.debug(f"Saved {len(points)} embeddings to Qdrant")
        except Exception as e:
            logger.error(f"Failed to save embeddings to Qdrant: {e}")
            await self._log_qdrant_failure(embeddings, str(e))
            raise  # 실패 시 예외 전파
    
    async def update_chunk_embedding_info(
        self,
        chunk_id: str,
        embedding_id: str,
        transaction: Optional[AsyncIOMotorClientSession] = None
    ):
        """청크에 임베딩 정보 업데이트"""
        update_data = {
            "$set": {
                "embedding_id": embedding_id,
                "indexed_at": datetime.now(timezone.utc)
            }
        }
        
        if transaction:
            result = await self.mongo.db[self.chunk_collection].update_one(
                {"_id": chunk_id},
                update_data,
                session=transaction
            )
        else:
            result = await self.mongo.db[self.chunk_collection].update_one(
                {"_id": chunk_id},
                update_data
            )
        
        return result.modified_count > 0
    
    async def update_processing_status(
        self, 
        document_id: str, 
        status: str, 
        error_message: str = None,
        processed_info: Dict[str, Any] = None
    ):
        """처리 상태 업데이트"""
        update_data = {
            "status": status,
            "updated_at": datetime.now(timezone.utc)
        }
        
        if error_message:
            update_data["error_message"] = error_message
        
        if processed_info:
            update_data["processed_info"] = processed_info
        
        result = await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
        
        if result:
            logger.info(f"Processing status updated for document: {document_id} to {status}")
    
    async def get_chunks_by_vector_search(
        self, 
        query_vector: List[float], 
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """벡터 검색 후 MongoDB에서 청크 조회"""
        # 1. Qdrant 검색
        search_results = await self.qdrant.search(
            query_vector=query_vector,
            limit=limit
        )
        
        # 2. MongoDB에서 청크 조회 (배치)
        chunk_ids = [r["payload"]["mongo_chunk_id"] for r in search_results]
        
        cursor = self.mongo.db[self.chunk_collection].find(
            {"_id": {"$in": chunk_ids}}
        )
        
        chunks_map = {}
        async for chunk in cursor:
            chunks_map[chunk["_id"]] = chunk
        
        # 3. 검색 순서대로 결과 정렬
        results = []
        for search_result in search_results:
            chunk_id = search_result["payload"]["mongo_chunk_id"]
            if chunk_id in chunks_map:
                chunk = chunks_map[chunk_id]
                chunk["search_score"] = search_result["score"]
                results.append(chunk)
        
        return results
    
    async def _log_qdrant_failure(self, embeddings: List[EmbeddingData], error: str):
        """Qdrant 실패 로그 저장"""
        failure_log = {
            "document_ids": list(set(e.content_id for e in embeddings)),
            "chunk_count": len(embeddings),
            "error": error,
            "failed_at": datetime.now(timezone.utc)
        }
        
        await self.mongo.insert_one("qdrant_failures", failure_log)
# content_pdf/repository.py
from typing import List, Dict, Any
from datetime import datetime, timezone
import logging
import asyncio

from infra.databases.qdrant_db import QdrantDB
from infra.databases.mongo_db import MongoDB
from schema import EmbeddingData, ChunkDocument

logger = logging.getLogger(__name__)

class PdfRepository:
    """PDF 모듈 데이터 접근 계층"""
    
    def __init__(self):
        self.qdrant = QdrantDB()
        self.mongo = MongoDB()
    
    async def save_chunks_to_mongo(self, chunks: List[ChunkDocument]):
        """청크를 MongoDB에 비동기로 저장"""
        documents = []
        for chunk in chunks:
            doc = {
                "document_id": chunk.document_id,
                "chunk_id": chunk.chunk_id,
                "chunk_data": chunk.chunk_data,
                "chunk_index": chunk.chunk_index,
                "created_at": chunk.created_at,
                "indexed_at": chunk.indexed_at,
                "index_info": chunk.index_info
            }
            documents.append(doc)
        
        # 비동기 벌크 삽입
        if documents:
            result = await self.mongo.db["chunk_documents"].insert_many(documents)
            logger.info(f"Saved {len(result.inserted_ids)} chunks to MongoDB")
    
    async def update_chunk_index_info(self, chunk_id: str, index_info: Dict[str, Any]):
        """청크의 인덱싱 정보만 업데이트"""
        result = await self.mongo.update_one(
            collection="chunk_documents",
            filter={"chunk_id": chunk_id},
            update={
                "$set": {
                    "indexed_at": datetime.now(timezone.utc),
                    "index_info": index_info
                }
            }
        )
        
        if result:
            logger.debug(f"Updated index info for chunk: {chunk_id}")
        else:
            logger.warning(f"Failed to update index info for chunk: {chunk_id}")
    
    async def save_embeddings_to_qdrant(self, embeddings: List[EmbeddingData]):
        """Qdrant에 임베딩 저장 (MongoDB 참조 정보 포함)"""
        points = []
        
        for embedding in embeddings:
            point = {
                "id": embedding.embedding_id,
                "vector": embedding.embedding_vector,
                "payload": {
                    # MongoDB 참조 정보
                    "document_id": embedding.content_id,
                    "chunk_id": embedding.chunk_id,
                    "mongo_collection": "chunk_documents",
                    
                    # 텍스트 및 메타데이터
                    "text": embedding.embedding_text[:500],  # 검색용 텍스트 일부
                    "metadata": embedding.metadata,
                    "content_type": "pdf",
                    "created_at": datetime.now(timezone.utc).isoformat()
                }
            }
            points.append(point)
        
        await self.qdrant.upsert_points(points)
        logger.info(f"Saved {len(points)} embeddings to Qdrant")
    
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
        else:
            logger.warning(f"No document found with id: {document_id}")
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
import logging

from infra.databases.mongo_db import MongoDB
from infra.databases.qdrant_db import QdrantDB
from ..schema import ChunkDocument
from schema import EmbeddingData

logger = logging.getLogger(__name__)

class StorageService:
    """통합 저장 서비스"""
    
    def __init__(self):
        self.mongo = MongoDB()
        self.qdrant = QdrantDB()
        self.chunk_collection = "pdf_chunks"
    
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
                "embedding_id": None
            }
            documents.append(doc)
            chunk_ids.append(chunk.chunk_id)
        
        if documents:
            result = await self.mongo.db[self.chunk_collection].insert_many(documents)
            logger.debug(f"Saved {len(result.inserted_ids)} chunks to MongoDB")
        
        return chunk_ids
    
    async def save_embeddings(
        self,
        embeddings: List[EmbeddingData],
        chunks: List[ChunkDocument]
    ):
        """Qdrant에 임베딩 저장"""
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
        
        if points:
            await self.qdrant.upsert_points(points)
            logger.debug(f"Saved {len(points)} embeddings to Qdrant")
            
            # MongoDB에 임베딩 정보 업데이트
            for embedding in embeddings:
                await self.mongo.db[self.chunk_collection].update_one(
                    {"_id": embedding.chunk_id},
                    {"$set": {
                        "embedding_id": embedding.embedding_id,
                        "indexed_at": datetime.now(timezone.utc)
                    }}
                )
    
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
        
        await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
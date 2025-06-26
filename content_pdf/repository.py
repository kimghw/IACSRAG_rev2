# content_pdf/repository.py
class PdfRepository:
    """PDF 모듈 데이터 접근 계층"""
    
    def __init__(self):
        self.qdrant = QdrantDB()
        self.mongo = MongoDB()
    
    async def save_to_qdrant(self, embeddings: List[EmbeddingData]):
        """Qdrant에 임베딩 저장"""
        points = []
        
        for embedding in embeddings:
            point = {
                "id": embedding.embedding_id,
                "vector": embedding.embedding_vector,
                "payload": {
                    "content_id": embedding.content_id,
                    "chunk_id": embedding.chunk_id,
                    "text": embedding.embedding_text,
                    "metadata": embedding.metadata,
                    "content_type": "pdf",  # 타입 정보 추가
                    "created_at": datetime.utcnow().isoformat()
                }
            }
            points.append(point)
        
        await self.qdrant.upsert_points(points)
    
    async def update_processing_status(
        self, 
        document_id: str, 
        status: str, 
        error_message: str = None
    ):
        """처리 상태 업데이트 - MongoDB 기본 CRUD 활용"""
        update_data = {
            "status": status,
            "updated_at": datetime.utcnow()
        }
        
        if error_message:
            update_data["error_message"] = error_message
        
        # MongoDB의 기본 CRUD 사용
        await self.mongo.update_one(
            collection="uploads",
            filter={"document_id": document_id},
            update={"$set": update_data}
        )
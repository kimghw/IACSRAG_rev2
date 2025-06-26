from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance

from infra.core.config import settings

class QdrantDB:
    """Qdrant 벡터 데이터베이스 - 레이지 싱글톤"""
    
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = QdrantClient(
                url=settings.QDRANT_URL,
                api_key=settings.QDRANT_API_KEY if hasattr(settings, 'QDRANT_API_KEY') else None
            )
            self._ensure_collection()
    
    def _ensure_collection(self):
        """컬렉션 존재 확인 및 생성"""
        try:
            self._client.get_collection(settings.QDRANT_COLLECTION_NAME)
        except:
            self._client.create_collection(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=settings.QDRANT_VECTOR_SIZE,
                    distance=Distance.COSINE
                )
            )
    
    async def upsert_points(self, points: List[Dict[str, Any]]):
        """포인트 저장/업데이트"""
        self._client.upsert(
            collection_name=settings.QDRANT_COLLECTION_NAME,
            points=points
        )
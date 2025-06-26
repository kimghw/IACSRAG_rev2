# infra/databases/qdrant_db.py
from typing import List, Dict, Any
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from qdrant_client.http.exceptions import ResponseHandlingException
import logging

from infra.core.config import settings

logger = logging.getLogger(__name__)

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
            self._initialize_connection()
    
    def _initialize_connection(self):
        """연결 초기화"""
        try:
            self._client = QdrantClient(
                url=settings.QDRANT_URL,
                api_key=settings.QDRANT_API_KEY if hasattr(settings, 'QDRANT_API_KEY') else None,
                timeout=30,  # 30초 타임아웃
                retry_attempts=3  # 재시도 3회
            )
            self._ensure_collection()
            logger.info("Qdrant connection initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize Qdrant connection: {e}")
            raise
    
    def _ensure_collection(self):
        """컬렉션 존재 확인 및 생성"""
        try:
            self._client.get_collection(settings.QDRANT_COLLECTION_NAME)
            logger.info(f"Collection '{settings.QDRANT_COLLECTION_NAME}' exists")
        except ResponseHandlingException:
            logger.info(f"Creating collection '{settings.QDRANT_COLLECTION_NAME}'")
            self._client.create_collection(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=settings.QDRANT_VECTOR_SIZE,
                    distance=Distance.COSINE
                )
            )
    
    async def ensure_connection(self):
        """연결 상태 확인"""
        try:
            # 컬렉션 정보로 연결 확인
            self._client.get_collection(settings.QDRANT_COLLECTION_NAME)
        except Exception as e:
            logger.warning(f"Qdrant connection issue, reinitializing: {e}")
            self._initialize_connection()
    
    async def upsert_points(self, points: List[Dict[str, Any]]):
        """포인트 저장/업데이트"""
        await self.ensure_connection()
        self._client.upsert(
            collection_name=settings.QDRANT_COLLECTION_NAME,
            points=points
        )
        logger.debug(f"Upserted {len(points)} points to Qdrant")
    
    async def close(self):
        """연결 종료"""
        if self._client:
            self._client.close()
            logger.info("Qdrant connection closed")
            self._client = None
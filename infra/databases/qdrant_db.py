# infra/databases/qdrant_db.py
from typing import List, Dict, Any, Optional
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
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
        """Qdrant 연결 초기화"""
        try:
            # 최신 버전의 qdrant-client는 retry_attempts 파라미터를 지원하지 않음
            self._client = QdrantClient(
                url=settings.QDRANT_URL,
                api_key=settings.QDRANT_API_KEY if hasattr(settings, 'QDRANT_API_KEY') and settings.QDRANT_API_KEY else None,
                timeout=30.0  # 타임아웃만 설정
            )
            self._ensure_collection()
            logger.info("Qdrant connection initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Qdrant connection: {str(e)}")
            raise
    
    def _ensure_collection(self):
        """컬렉션 존재 확인 및 생성"""
        try:
            # 컬렉션 목록 확인
            collections = self._client.get_collections()
            collection_names = [c.name for c in collections.collections]
            
            if settings.QDRANT_COLLECTION_NAME not in collection_names:
                # 컬렉션이 없으면 생성
                self._client.create_collection(
                    collection_name=settings.QDRANT_COLLECTION_NAME,
                    vectors_config=VectorParams(
                        size=settings.QDRANT_VECTOR_SIZE,
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Created Qdrant collection: {settings.QDRANT_COLLECTION_NAME}")
            else:
                logger.info(f"Qdrant collection already exists: {settings.QDRANT_COLLECTION_NAME}")
                
        except Exception as e:
            logger.error(f"Failed to ensure collection: {str(e)}")
            raise
    
    async def upsert_points(self, points: List[Dict[str, Any]]):
        """포인트 저장/업데이트"""
        try:
            # PointStruct 객체로 변환
            point_structs = []
            for point in points:
                point_struct = PointStruct(
                    id=point["id"],
                    vector=point["vector"],
                    payload=point.get("payload", {})
                )
                point_structs.append(point_struct)
            
            # 동기 메서드 사용 (qdrant-client는 기본적으로 동기)
            self._client.upsert(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                points=point_structs
            )
            logger.info(f"Upserted {len(points)} points to Qdrant")
            
        except Exception as e:
            logger.error(f"Failed to upsert points: {str(e)}")
            raise
    
    async def search(
        self, 
        query_vector: List[float], 
        limit: int = 10,
        filter: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """벡터 검색"""
        try:
            results = self._client.search(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                query_vector=query_vector,
                limit=limit,
                query_filter=filter
            )
            
            return [
                {
                    "id": hit.id,
                    "score": hit.score,
                    "payload": hit.payload
                }
                for hit in results
            ]
            
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            raise
    
    async def get_point(self, point_id: str) -> Optional[Dict[str, Any]]:
        """특정 포인트 조회"""
        try:
            points = self._client.retrieve(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                ids=[point_id]
            )
            
            if points:
                point = points[0]
                return {
                    "id": point.id,
                    "vector": point.vector,
                    "payload": point.payload
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to get point: {str(e)}")
            return None
    
    async def delete_points(self, point_ids: List[str]):
        """포인트 삭제"""
        try:
            self._client.delete(
                collection_name=settings.QDRANT_COLLECTION_NAME,
                points_selector={"points": point_ids}
            )
            logger.info(f"Deleted {len(point_ids)} points from Qdrant")
            
        except Exception as e:
            logger.error(f"Failed to delete points: {str(e)}")
            raise
    
    def close(self):
        """연결 종료"""
        if self._client:
            # qdrant-client는 명시적인 close 메서드가 없음
            self._client = None
            logger.info("Qdrant connection closed")
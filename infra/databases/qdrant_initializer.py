# infra/databases/qdrant_initializer.py
import logging
import httpx
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance
from infra.core.docker_manager import ServiceInitializer
from infra.core.config import settings

logger = logging.getLogger(__name__)

class QdrantInitializer(ServiceInitializer):
    """Qdrant 초기화 관리"""
    
    def __init__(self):
        super().__init__('iacsrag_qdrant', 'qdrant')
        self.url = settings.QDRANT_URL
        self.collection_name = settings.QDRANT_COLLECTION_NAME
        self.vector_size = settings.QDRANT_VECTOR_SIZE
    
    async def test_connection(self) -> bool:
        """Qdrant 연결 테스트"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.url}/collections", timeout=5.0)
                return response.status_code == 200
        except Exception as e:
            logger.debug(f"Qdrant connection test failed: {e}")
            return False
    
    async def initialize(self) -> bool:
        """Qdrant 초기화"""
        try:
            client = QdrantClient(url=self.url)
            
            # 컬렉션 확인 및 생성
            try:
                client.get_collection(self.collection_name)
                logger.info(f"Collection '{self.collection_name}' already exists")
            except:
                client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE
                    )
                )
                logger.info(f"Created collection: {self.collection_name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Qdrant initialization failed: {e}")
            return False
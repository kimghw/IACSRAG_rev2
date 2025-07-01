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
        self.vector_size = settings.QDRANT_VECTOR_SIZE
        
        # 초기화할 컬렉션 목록
        self.collections = [
            settings.QDRANT_COLLECTION_NAME,  # 기본 컬렉션 (PDF, Markdown 등)
            settings.QDRANT_EMAIL_COLLECTION   # 이메일 전용 컬렉션
        ]
    
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
        """Qdrant 초기화 - 모든 필요한 컬렉션 생성"""
        try:
            client = QdrantClient(url=self.url)
            
            # 현재 존재하는 컬렉션 목록 가져오기
            collections = client.get_collections()
            existing_names = [c.name for c in collections.collections]
            
            # 각 컬렉션 확인 및 생성
            for collection_name in self.collections:
                if collection_name in existing_names:
                    # 이미 존재하면 정보만 출력
                    logger.info(f"Collection '{collection_name}' already exists - skipping creation")
                    
                    try:
                        col_info = client.get_collection(collection_name)
                        logger.info(f"  - Status: {col_info.status}")
                        logger.info(f"  - Points count: {col_info.points_count}")
                        logger.info(f"  - Vectors count: {col_info.vectors_count}")
                        logger.info(f"  - Vector size: {col_info.config.params.vectors.size}")
                    except Exception as e:
                        logger.debug(f"Could not get collection info: {e}")
                else:
                    # 컬렉션이 없으면 생성
                    logger.info(f"Creating new collection: {collection_name}")
                    client.create_collection(
                        collection_name=collection_name,
                        vectors_config=VectorParams(
                            size=self.vector_size,
                            distance=Distance.COSINE
                        )
                    )
                    logger.info(f"✅ Created collection: {collection_name}")
                    logger.info(f"  - Vector size: {self.vector_size}")
                    logger.info(f"  - Distance metric: COSINE")
            
            # 컬렉션별 용도 설명
            logger.info("\n📁 Collection Summary:")
            logger.info(f"  - '{settings.QDRANT_COLLECTION_NAME}': PDF, Markdown, and other documents")
            logger.info(f"  - '{settings.QDRANT_EMAIL_COLLECTION}': Email documents only")
            
            return True
            
        except Exception as e:
            logger.error(f"Qdrant initialization failed: {e}")
            return False

# infra/databases/mongo_initializer.py
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from infra.core.docker_manager import ServiceInitializer
from infra.core.config import settings

logger = logging.getLogger(__name__)

class MongoInitializer(ServiceInitializer):
    """MongoDB 초기화 관리"""
    
    def __init__(self):
        super().__init__('iacsrag_mongodb', 'mongodb')
        self.connection_string = settings.MONGODB_URL
        self.database_name = settings.MONGODB_DATABASE
    
    async def test_connection(self) -> bool:
        """MongoDB 연결 테스트"""
        try:
            client = AsyncIOMotorClient(
                self.connection_string,
                serverSelectionTimeoutMS=5000
            )
            await client.admin.command('ping')
            client.close()
            return True
        except Exception as e:
            logger.debug(f"MongoDB connection test failed: {e}")
            return False
    
  # infra/databases/mongo_initializer.py
async def initialize(self) -> bool:
    """MongoDB 초기화"""
    try:
        client = AsyncIOMotorClient(self.connection_string)
        db = client[self.database_name]
        
        # 필요한 컬렉션 생성
        collections = [
            'uploads',           # 업로드 정보
            'pdf_chunks',        # PDF 청크
            'processing_logs',   # 처리 로그
            'qdrant_failures'    # Qdrant 실패 로그
        ]
        existing = await db.list_collection_names()
        
        for collection in collections:
            if collection not in existing:
                await db.create_collection(collection)
                logger.info(f"Created collection: {collection}")
        
        # 인덱스 생성
        await db.uploads.create_index('document_id', unique=True)
        await db.uploads.create_index('quick_hash')
        
        # PDF 청크 인덱스
        await db.pdf_chunks.create_index('_id')  # 기본 인덱스
        await db.pdf_chunks.create_index('document_id')
        await db.pdf_chunks.create_index([('document_id', 1), ('chunk_index', 1)])
        await db.pdf_chunks.create_index('embedding_id')
        
        logger.info("MongoDB indexes created")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"MongoDB initialization failed: {e}")
        return False
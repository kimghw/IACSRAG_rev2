# infra/databases/mongo_db.py
from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError, ConnectionFailure
import logging
import asyncio

from infra.core.config import settings

logger = logging.getLogger(__name__)

class MongoDB:
    """MongoDB 연결 관리 - 레이지 싱글톤"""
    _instance = None
    _client = None
    _db = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._initialize_connection()
    
    def _initialize_connection(self):
        """연결 초기화 with 연결 풀 설정"""
        try:
            self._client = AsyncIOMotorClient(
                settings.MONGODB_URL,
                maxPoolSize=10,  # 최대 연결 수
                minPoolSize=2,   # 최소 연결 수
                maxIdleTimeMS=30000,  # 30초 유휴 시 연결 해제
                connectTimeoutMS=5000,  # 연결 타임아웃 5초
                serverSelectionTimeoutMS=5000,
                retryWrites=True,
                retryReads=True
            )
            self._db = self._client[settings.MONGODB_DATABASE]
            logger.info("MongoDB connection pool initialized")
            
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB connection: {e}")
            raise
    
    async def ensure_connection(self):
        """연결 상태 확인 및 재연결"""
        try:
            # ping으로 연결 확인
            await self._client.admin.command('ping')
        except (ConnectionFailure, PyMongoError) as e:
            logger.warning(f"MongoDB connection lost, reconnecting: {e}")
            self._initialize_connection()
    
    @property
    def db(self):
        """데이터베이스 인스턴스 반환"""
        return self._db
    
    @property
    def client(self):
        """클라이언트 인스턴스 반환 (트랜잭션용)"""
        return self._client
    
    # 기본 CRUD 메서드들 with 연결 확인
    async def insert_one(self, collection: str, document: Dict[str, Any]) -> str:
        """단일 문서 삽입"""
        try:
            await self.ensure_connection()  # 연결 확인
            result = await self._db[collection].insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Insert failed: {str(e)}")
            raise
    
    async def find_one(self, collection: str, filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 문서 조회"""
        await self.ensure_connection()
        return await self._db[collection].find_one(filter)
    
    async def update_one(self, collection: str, filter: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """단일 문서 업데이트"""
        await self.ensure_connection()
        result = await self._db[collection].update_one(filter, update)
        return result.modified_count > 0
    
    async def close(self):
        """연결 종료"""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")
            self._client = None
            self._db = None
# infra/databases/mongo_db.py
from typing import Dict, Any, List, Optional
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import PyMongoError
import logging

from infra.core.config import settings

logger = logging.getLogger(__name__)

class MongoDB:
    """MongoDB 기본 CRUD 제공 - 레이지 싱글톤"""
    _instance = None
    _client = None
    _db = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._client = AsyncIOMotorClient(settings.MONGODB_URL)
            self._db = self._client[settings.MONGODB_DATABASE]
            logger.info("MongoDB connection initialized")
    
    @property
    def db(self):
        """데이터베이스 인스턴스 반환"""
        return self._db
    
    # 기본 CRUD 메서드들
    async def insert_one(self, collection: str, document: Dict[str, Any]) -> str:
        """단일 문서 삽입"""
        try:
            result = await self._db[collection].insert_one(document)
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Insert failed: {str(e)}")
            raise
    
    async def find_one(self, collection: str, filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 문서 조회"""
        return await self._db[collection].find_one(filter)
    
    async def update_one(self, collection: str, filter: Dict[str, Any], update: Dict[str, Any]) -> bool:
        """단일 문서 업데이트"""
        result = await self._db[collection].update_one(filter, update)
        return result.modified_count > 0
    
    async def delete_one(self, collection: str, filter: Dict[str, Any]) -> bool:
        """단일 문서 삭제"""
        result = await self._db[collection].delete_one(filter)
        return result.deleted_count > 0
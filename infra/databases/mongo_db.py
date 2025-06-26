# infra/databases/mongo_db.py
class MongoDB:
    """MongoDB 기본 CRUD 제공"""
    _instance = None
    _client = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def insert_one(self, collection: str, document: Dict[str, Any]):
        """단일 문서 삽입"""
        return await self.db[collection].insert_one(document)
    
    async def find_one(self, collection: str, filter: Dict[str, Any]):
        """단일 문서 조회"""
        return await self.db[collection].find_one(filter)
    
    async def update_one(self, collection: str, filter: Dict[str, Any], update: Dict[str, Any]):
        """단일 문서 업데이트"""
        return await self.db[collection].update_one(filter, update)
    
    async def delete_one(self, collection: str, filter: Dict[str, Any]):
        """단일 문서 삭제"""
        return await self.db[collection].delete_one(filter)
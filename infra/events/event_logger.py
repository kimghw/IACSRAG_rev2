# infra/events/event_logger.py
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from infra.databases.mongo_db import MongoDB

logger = logging.getLogger(__name__)

class EventLogger:
    """간단한 이벤트 처리 로그 기록기"""
    
    def __init__(self):
        self.mongo = MongoDB()
    
    async def log_event_start(self, topic: str, event_data: Dict[str, Any]) -> str:
        """이벤트 처리 시작 로그"""
        try:
            log_entry = {
                'topic': topic,
                'event_type': event_data.get('event_type', 'unknown'),
                'document_id': event_data.get('document_id'),
                'event_id': event_data.get('event_id'),
                'status': 'processing',
                'started_at': datetime.utcnow(),
                'event_data': event_data
            }
            
            result = await self.mongo.db.event_logs.insert_one(log_entry)
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Failed to log event start: {e}")
            return None
    
    async def log_event_complete(self, log_id: str):
        """이벤트 처리 완료 로그"""
        if not log_id:
            return
            
        try:
            await self.mongo.db.event_logs.update_one(
                {'_id': log_id},
                {
                    '$set': {
                        'status': 'completed',
                        'completed_at': datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Failed to log event complete: {e}")
    
    async def log_event_failed(self, log_id: str, error: str):
        """이벤트 처리 실패 로그"""
        if not log_id:
            return
            
        try:
            await self.mongo.db.event_logs.update_one(
                {'_id': log_id},
                {
                    '$set': {
                        'status': 'failed',
                        'error': error,
                        'failed_at': datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Failed to log event failure: {e}")

# 전역 인스턴스
event_logger = EventLogger()
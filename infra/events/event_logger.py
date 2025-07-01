# infra/events/event_logger.py
from datetime import datetime, timedelta
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from infra.databases.mongo_db import MongoDB

logger = logging.getLogger(__name__)

class EventLogger:
    """통합 이벤트 처리 로그 기록기"""
    
    def __init__(self):
        self.mongo = MongoDB()
    
    # === 기존 일반 이벤트 로깅 ===
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
    
    # === 이메일 이벤트 전용 로깅 ===
    async def log_email_event_start(
        self, 
        topic: str, 
        event_data: Dict[str, Any],
        batch_mode: bool = False
    ) -> str:
        """이메일 이벤트 처리 시작 로그"""
        try:
            log_entry = {
                'topic': topic,
                'event_type': 'email',
                'event_id': event_data.get('event_id'),
                'account_id': event_data.get('account_id'),
                'email_count': event_data.get('response_data', {}).get('value', []).__len__(),
                'batch_mode': batch_mode,
                'status': 'processing',
                'started_at': datetime.utcnow(),
                'event_data': event_data
            }
            
            result = await self.mongo.db.email_event_logs.insert_one(log_entry)
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Failed to log email event start: {e}")
            return None
    
    # === 배치 처리 로깅 ===
    async def log_batch_start(
        self,
        batch_id: str,
        event_count: int,
        event_type: str
    ) -> str:
        """배치 처리 시작 로그"""
        try:
            log_entry = {
                'batch_id': batch_id,
                'event_type': event_type,
                'event_count': event_count,
                'status': 'processing',
                'started_at': datetime.utcnow()
            }
            
            result = await self.mongo.db.batch_logs.insert_one(log_entry)
            return str(result.inserted_id)
            
        except Exception as e:
            logger.error(f"Failed to log batch start: {e}")
            return None
    
    async def log_batch_complete(
        self,
        batch_log_id: str,
        processed_count: int,
        processing_time: float
    ):
        """배치 처리 완료 로그"""
        if not batch_log_id:
            return
            
        try:
            await self.mongo.db.batch_logs.update_one(
                {'_id': batch_log_id},
                {
                    '$set': {
                        'status': 'completed',
                        'processed_count': processed_count,
                        'processing_time': processing_time,
                        'completed_at': datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Failed to log batch complete: {e}")
    
    async def log_batch_failed(self, batch_log_id: str, error: str):
        """배치 처리 실패 로그"""
        if not batch_log_id:
            return
            
        try:
            await self.mongo.db.batch_logs.update_one(
                {'_id': batch_log_id},
                {
                    '$set': {
                        'status': 'failed',
                        'error': error,
                        'failed_at': datetime.utcnow()
                    }
                }
            )
        except Exception as e:
            logger.error(f"Failed to log batch failure: {e}")
    
    # === 이메일 처리 상세 로깅 ===
    async def log_email_processing_details(
        self,
        document_id: str,
        email_id: str,
        status: str,
        details: Dict[str, Any]
    ):
        """이메일 처리 상세 로그"""
        try:
            log_entry = {
                'document_id': document_id,
                'email_id': email_id,
                'status': status,
                'details': details,
                'timestamp': datetime.utcnow()
            }
            
            await self.mongo.db.email_processing_details.insert_one(log_entry)
            
        except Exception as e:
            logger.error(f"Failed to log email processing details: {e}")
    
    # === 통계 조회 ===
    async def get_email_stats(self, hours: int = 24) -> Dict[str, Any]:
        """이메일 처리 통계 조회"""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            
            # 이메일 이벤트 통계
            pipeline = [
                {'$match': {
                    'started_at': {'$gte': since},
                    'event_type': 'email'
                }},
                {'$group': {
                    '_id': '$status',
                    'count': {'$sum': 1},
                    'total_emails': {'$sum': '$email_count'}
                }}
            ]
            
            results = await self.mongo.db.email_event_logs.aggregate(pipeline).to_list(None)
            
            stats = {
                'period_hours': hours,
                'by_status': {},
                'total_events': 0,
                'total_emails': 0
            }
            
            for result in results:
                status = result['_id']
                stats['by_status'][status] = {
                    'events': result['count'],
                    'emails': result.get('total_emails', 0)
                }
                stats['total_events'] += result['count']
                stats['total_emails'] += result.get('total_emails', 0)
            
            # 배치 처리 통계
            batch_pipeline = [
                {'$match': {
                    'started_at': {'$gte': since},
                    'event_type': 'email'
                }},
                {'$group': {
                    '_id': '$status',
                    'count': {'$sum': 1},
                    'avg_processing_time': {'$avg': '$processing_time'},
                    'total_processed': {'$sum': '$processed_count'}
                }}
            ]
            
            batch_results = await self.mongo.db.batch_logs.aggregate(batch_pipeline).to_list(None)
            
            stats['batch_stats'] = {}
            for result in batch_results:
                stats['batch_stats'][result['_id']] = {
                    'batch_count': result['count'],
                    'avg_processing_time': result.get('avg_processing_time', 0),
                    'total_processed': result.get('total_processed', 0)
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get email stats: {e}")
            return {}

# 전역 인스턴스
event_logger = EventLogger()

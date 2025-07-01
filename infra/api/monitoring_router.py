# infra/api/monitoring_router.py
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import logging
import json

from infra.databases.mongo_db import MongoDB
from infra.core.config import settings

router = APIRouter(prefix="/api/v1/monitoring", tags=["monitoring"])
logger = logging.getLogger(__name__)

@router.get("/events/stats")
async def get_event_stats(
    hours: int = Query(24, description="통계 기간 (시간)"),
    group_id: Optional[str] = Query(None, description="Consumer Group ID")
) -> Dict[str, Any]:
    """이벤트 처리 통계 조회"""
    try:
        mongo = MongoDB()
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # 기본 필터
        match_filter = {'started_at': {'$gte': since}}
        if group_id:
            match_filter['group_id'] = group_id
        
        # 전체 통계
        pipeline = [
            {'$match': match_filter},
            {'$group': {
                '_id': {
                    'status': '$status',
                    'topic': '$topic'
                },
                'count': {'$sum': 1},
                'avg_duration': {
                    '$avg': {
                        '$subtract': [
                            '$completed_at',
                            '$started_at'
                        ]
                    }
                }
            }}
        ]
        
        results = await mongo.db.event_logs.aggregate(pipeline).to_list(None)
        
        # 결과 포맷팅
        stats = {}
        for result in results:
            topic = result['_id']['topic']
            status = result['_id']['status']
            
            if topic not in stats:
                stats[topic] = {}
            
            stats[topic][status] = {
                'count': result['count'],
                'avg_duration_ms': result['avg_duration'] if result['avg_duration'] else 0
            }
        
        return {
            'period_hours': hours,
            'since': since.isoformat(),
            'stats': stats
        }
        
    except Exception as e:
        logger.error(f"Failed to get event stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/events/failed")
async def get_failed_events(
    hours: int = Query(24, description="조회 기간 (시간)"),
    limit: int = Query(100, description="최대 결과 수")
) -> List[Dict[str, Any]]:
    """실패한 이벤트 목록 조회"""
    try:
        mongo = MongoDB()
        since = datetime.utcnow() - timedelta(hours=hours)
        
        failed_events = await mongo.db.event_logs.find({
            'status': 'failed',
            'started_at': {'$gte': since}
        }).sort('started_at', -1).limit(limit).to_list(None)
        
        # ObjectId를 문자열로 변환
        for event in failed_events:
            event['_id'] = str(event['_id'])
        
        return failed_events
        
    except Exception as e:
        logger.error(f"Failed to get failed events: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/dlq/messages")
async def get_dlq_messages(
    topic: str = Query(..., description="DLQ 토픽 이름"),
    limit: int = Query(50, description="최대 결과 수")
) -> List[Dict[str, Any]]:
    """Dead Letter Queue 메시지 조회"""
    try:
        from aiokafka import AIOKafkaConsumer
        import asyncio
        
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000  # 5초 타임아웃
        )
        
        await consumer.start()
        
        messages = []
        try:
            async for msg in consumer:
                messages.append({
                    'partition': msg.partition,
                    'offset': msg.offset,
                    'key': msg.key.decode() if msg.key else None,
                    'value': json.loads(msg.value.decode()),
                    'timestamp': msg.timestamp
                })
                
                if len(messages) >= limit:
                    break
                    
        finally:
            await consumer.stop()
        
        return messages
        
    except Exception as e:
        logger.error(f"Failed to get DLQ messages: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/events/retry/{event_id}")
async def retry_event(event_id: str) -> Dict[str, str]:
    """실패한 이벤트 재시도"""
    try:
        mongo = MongoDB()
        
        # 이벤트 로그 조회
        event_log = await mongo.db.event_logs.find_one({'_id': event_id})
        if not event_log:
            raise HTTPException(status_code=404, detail="Event not found")
        
        if event_log['status'] != 'failed':
            raise HTTPException(status_code=400, detail="Only failed events can be retried")
        
        # 원본 메시지 재발행
        from infra.events.event_producer import EventProducer
        producer = EventProducer()
        
        # 재시도 카운트 추가
        message = event_log['message_data'].copy()
        message['_retry_count'] = message.get('_retry_count', 0) + 1
        message['_manual_retry'] = True
        message['_retry_by'] = 'monitoring_api'
        
        await producer.send_event(
            topic=event_log['topic'],
            key=event_log['key'],
            value=message
        )
        
        # 이벤트 로그 업데이트
        await mongo.db.event_logs.update_one(
            {'_id': event_id},
            {'$set': {
                'retried_at': datetime.utcnow(),
                'status': 'retrying'
            }}
        )
        
        return {
            'status': 'success',
            'message': f'Event {event_id} queued for retry'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to retry event: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/health/consumers")
async def check_consumer_health() -> Dict[str, Any]:
    """Consumer 상태 확인"""
    try:
        mongo = MongoDB()
        
        # 최근 5분간의 처리 확인
        since = datetime.utcnow() - timedelta(minutes=5)
        
        pipeline = [
            {'$match': {
                'started_at': {'$gte': since}
            }},
            {'$group': {
                '_id': '$group_id',
                'last_processed': {'$max': '$started_at'},
                'processing_count': {
                    '$sum': {
                        '$cond': [{'$eq': ['$status', 'processing']}, 1, 0]
                    }
                },
                'completed_count': {
                    '$sum': {
                        '$cond': [{'$eq': ['$status', 'completed']}, 1, 0]
                    }
                }
            }}
        ]
        
        results = await mongo.db.event_logs.aggregate(pipeline).to_list(None)
        
        health_status = {}
        for result in results:
            health_status[result['_id']] = {
                'status': 'healthy' if result['completed_count'] > 0 else 'degraded',
                'last_processed': result['last_processed'].isoformat(),
                'processing': result['processing_count'],
                'completed_last_5min': result['completed_count']
            }
        
        return health_status
        
    except Exception as e:
        logger.error(f"Failed to check consumer health: {e}")
        raise HTTPException(status_code=500, detail=str(e))
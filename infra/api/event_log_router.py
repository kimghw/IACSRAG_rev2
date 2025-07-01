# infra/api/event_log_router.py
from fastapi import APIRouter, HTTPException, Query
from typing import Dict, Any, List
from datetime import datetime, timedelta
import logging

from infra.databases.mongo_db import MongoDB

router = APIRouter(prefix="/api/v1/events", tags=["event-logs"])
logger = logging.getLogger(__name__)

@router.get("/logs")
async def get_event_logs(
    hours: int = Query(24, description="최근 N시간의 로그"),
    status: str = Query(None, description="상태 필터 (processing/completed/failed)"),
    limit: int = Query(100, description="최대 결과 수")
) -> List[Dict[str, Any]]:
    """이벤트 처리 로그 조회"""
    try:
        mongo = MongoDB()
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # 필터 구성
        filter_query = {'started_at': {'$gte': since}}
        if status:
            filter_query['status'] = status
        
        # 로그 조회
        logs = await mongo.db.event_logs.find(filter_query)\
            .sort('started_at', -1)\
            .limit(limit)\
            .to_list(None)
        
        # ObjectId를 문자열로 변환
        for log in logs:
            log['_id'] = str(log['_id'])
        
        return logs
        
    except Exception as e:
        logger.error(f"Failed to get event logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats")
async def get_event_stats(
    hours: int = Query(24, description="통계 기간 (시간)")
) -> Dict[str, Any]:
    """이벤트 처리 통계"""
    try:
        mongo = MongoDB()
        since = datetime.utcnow() - timedelta(hours=hours)
        
        # 상태별 카운트
        pipeline = [
            {'$match': {'started_at': {'$gte': since}}},
            {'$group': {
                '_id': {
                    'status': '$status',
                    'event_type': '$event_type'
                },
                'count': {'$sum': 1}
            }}
        ]
        
        results = await mongo.db.event_logs.aggregate(pipeline).to_list(None)
        
        # 결과 정리
        stats = {
            'period_hours': hours,
            'total': 0,
            'by_status': {},
            'by_type': {}
        }
        
        for result in results:
            count = result['count']
            status = result['_id']['status']
            event_type = result['_id'].get('event_type', 'unknown')
            
            # 전체 카운트
            stats['total'] += count
            
            # 상태별 카운트
            if status not in stats['by_status']:
                stats['by_status'][status] = 0
            stats['by_status'][status] += count
            
            # 타입별 카운트
            if event_type not in stats['by_type']:
                stats['by_type'][event_type] = 0
            stats['by_type'][event_type] += count
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get event stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))
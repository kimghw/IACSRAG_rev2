# content_email/email_batch_processor.py
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

from infra.events.event_logger import event_logger

logger = logging.getLogger(__name__)

class EmailBatchProcessor:
    """이메일 배치 프로세서 - 이벤트를 누적하여 배치 처리"""
    
    def __init__(
        self,
        batch_size: int,
        orchestrator: 'EmailOrchestrator',  # 타입 힌트 추가
        max_wait_time: float = 5.0
    ):
        self.batch_size = batch_size
        self.orchestrator = orchestrator
        self.max_wait_time = max_wait_time
        
        self.current_batch = []
        self.batch_start_time = None
        self.processing_task = None
        self.lock = asyncio.Lock()
        self.wait_task = None
        
        # 통계
        self.stats = {
            'total_batches_processed': 0,
            'total_emails_processed': 0,
            'failed_batches': 0,
            'average_batch_size': 0
        }
        
        logger.info(f"EmailBatchProcessor initialized")
        logger.info(f"  - Batch size: {batch_size}")
        logger.info(f"  - Max wait time: {max_wait_time}s")
    
    async def add_event(self, event_data: Dict[str, Any]):
        """이벤트를 배치에 추가"""
        async with self.lock:
            # 첫 이벤트면 타이머 시작
            if not self.current_batch:
                self.batch_start_time = asyncio.get_event_loop().time()
                # 타임아웃 태스크 시작
                if self.wait_task:
                    self.wait_task.cancel()
                self.wait_task = asyncio.create_task(self._wait_and_flush())
            
            self.current_batch.append(event_data)
            
            # 배치가 가득 찼으면 즉시 처리
            if len(self.current_batch) >= self.batch_size:
                logger.info(f"Batch full ({len(self.current_batch)} events), triggering processing")
                await self._trigger_batch_processing()
    
    async def _wait_and_flush(self):
        """타임아웃 후 자동 플러시"""
        try:
            await asyncio.sleep(self.max_wait_time)
            async with self.lock:
                if self.current_batch:
                    logger.info(f"Timeout reached ({self.max_wait_time}s), flushing {len(self.current_batch)} events")
                    await self._trigger_batch_processing()
        except asyncio.CancelledError:
            pass
    
    async def _trigger_batch_processing(self):
        """배치 처리 트리거 - lock 내부에서 호출됨"""
        if not self.current_batch:
            return
        
        # 현재 배치를 처리용으로 복사
        batch_to_process = self.current_batch[:]
        self.current_batch = []
        self.batch_start_time = None
        
        # 타임아웃 태스크 취소
        if self.wait_task:
            self.wait_task.cancel()
            self.wait_task = None
        
        # 백그라운드에서 처리
        self.processing_task = asyncio.create_task(
            self._process_batch(batch_to_process)
        )
    
    async def _process_batch(self, events: List[Dict[str, Any]]):
        """배치 처리 - orchestrator의 배치 처리 메서드 호출"""
        batch_id = f"batch_{datetime.utcnow().timestamp()}"
        start_time = asyncio.get_event_loop().time()
        
        # 배치 처리 시작 로그
        batch_log_id = await event_logger.log_batch_start(
            batch_id=batch_id,
            event_count=len(events),
            event_type='email'
        )
        
        logger.info(f"Processing batch {batch_id} with {len(events)} events")
        
        try:
            # Orchestrator의 배치 처리 메서드 호출
            result = await self.orchestrator.process_batch_events(events)
            
            # 통계 업데이트
            self.stats['total_batches_processed'] += 1
            self.stats['total_emails_processed'] += result.get('processed_count', 0)
            
            # 평균 배치 크기 업데이트
            if self.stats['total_batches_processed'] > 0:
                self.stats['average_batch_size'] = (
                    self.stats['total_emails_processed'] / self.stats['total_batches_processed']
                )
            
            # 처리 시간
            processing_time = asyncio.get_event_loop().time() - start_time
            
            # 배치 처리 완료 로그
            await event_logger.log_batch_complete(
                batch_log_id=batch_log_id,
                processed_count=result.get('processed_count', 0),
                processing_time=processing_time
            )
            
            # 개별 이벤트 로그 업데이트
            for event in events:
                log_id = event.get('_log_id')
                if log_id:
                    await event_logger.log_event_complete(log_id)
            
            logger.info(f"Batch processing completed: {result.get('processed_count', 0)} emails in {processing_time:.2f}s")
            
        except Exception as e:
            self.stats['failed_batches'] += 1
            error_msg = f"Batch processing failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # 배치 실패 로그
            await event_logger.log_batch_failed(
                batch_log_id=batch_log_id,
                error=str(e)
            )
            
            # 개별 이벤트 실패 로그
            for event in events:
                log_id = event.get('_log_id')
                if log_id:
                    await event_logger.log_event_failed(log_id, error_msg)
    
    async def flush(self):
        """남은 이벤트 처리"""
        async with self.lock:
            if self.current_batch:
                logger.info(f"Flushing {len(self.current_batch)} remaining events")
                await self._trigger_batch_processing()
        
        # 모든 처리 완료 대기
        if self.processing_task:
            await self.processing_task
    
    def get_stats(self) -> Dict[str, Any]:
        """현재 통계 반환"""
        return {
            **self.stats,
            'current_batch_size': len(self.current_batch),
            'is_processing': self.processing_task and not self.processing_task.done()
        }
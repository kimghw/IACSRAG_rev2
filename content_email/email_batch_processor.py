# content_email/email_batch_processor.py
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class EmailBatchProcessor:
    """이메일 배치 프로세서 - 이벤트를 누적하여 배치 처리"""
    
    def __init__(
        self,
        batch_size: int,
        orchestrator,
        max_wait_time: float = 5.0  # 최대 대기 시간 (초)
    ):
        self.batch_size = batch_size
        self.orchestrator = orchestrator
        self.max_wait_time = max_wait_time
        
        self.current_batch = []
        self.batch_start_time = None
        self.processing_task = None
        self.lock = asyncio.Lock()
        
        logger.info(f"EmailBatchProcessor initialized with batch_size={batch_size}")
    
    async def add_event(self, event_data: Dict[str, Any]):
        """이벤트를 배치에 추가"""
        async with self.lock:
            # 첫 이벤트면 타이머 시작
            if not self.current_batch:
                self.batch_start_time = asyncio.get_event_loop().time()
            
            self.current_batch.append(event_data)
            
            # 배치가 가득 찼거나 시간이 초과되면 처리
            should_process = (
                len(self.current_batch) >= self.batch_size or
                (self.batch_start_time and 
                 asyncio.get_event_loop().time() - self.batch_start_time >= self.max_wait_time)
            )
            
            if should_process:
                await self._trigger_batch_processing()
    
    async def _trigger_batch_processing(self):
        """배치 처리 트리거"""
        if not self.current_batch:
            return
        
        # 현재 배치를 처리용으로 복사
        batch_to_process = self.current_batch[:]
        self.current_batch = []
        self.batch_start_time = None
        
        # 백그라운드에서 처리
        self.processing_task = asyncio.create_task(
            self._process_batch(batch_to_process)
        )
    
    async def _process_batch(self, events: List[Dict[str, Any]]):
        """배치 처리"""
        logger.info(f"Processing batch of {len(events)} events")
        
        try:
            # 모든 이벤트의 이메일을 하나로 합침
            all_emails = []
            event_map = {}  # email_id -> event_id 매핑
            
            for event in events:
                event_id = event.get('event_id')
                account_id = event.get('account_id')
                emails = event.get('response_data', {}).get('value', [])
                
                for email in emails:
                    # 이메일에 메타데이터 추가
                    email['_event_id'] = event_id
                    email['_account_id'] = account_id
                    all_emails.append(email)
                    event_map[email['id']] = event_id
            
            logger.info(f"Total emails in batch: {len(all_emails)}")
            
            # 오케스트레이터에 배치 처리 요청
            from .schema import EmailProcessingRequest
            
            # 통합 요청 생성
            batch_request = EmailProcessingRequest(
                event_id=f"batch_{datetime.utcnow().timestamp()}",
                account_id="batch_processing",
                event_data={
                    'response_data': {'value': all_emails},
                    'event_map': event_map
                }
            )
            
            # 처리
            result = await self.orchestrator.process_email(batch_request)
            
            logger.info(f"Batch processing completed: {result.email_count} emails")
            
        except Exception as e:
            logger.error(f"Batch processing failed: {str(e)}", exc_info=True)
    
    async def flush(self):
        """남은 이벤트 처리"""
        async with self.lock:
            if self.current_batch:
                logger.info(f"Flushing {len(self.current_batch)} remaining events")
                await self._trigger_batch_processing()
        
        # 모든 처리 완료 대기
        if self.processing_task:
            await self.processing_task
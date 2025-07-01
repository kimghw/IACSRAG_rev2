# infra/events/email_event_consumer_v2.py
import logging
import json
import asyncio
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer

from infra.core.config import settings
from content_email.email_batch_processor import EmailBatchProcessor
from content_email.orchestrator import EmailOrchestrator

logger = logging.getLogger(__name__)

class EmailEventConsumerV2:
    """이메일 이벤트 컨슈머 - 배치 처리 버전"""
    
    def __init__(self):
        self.consumer = None
        self._running = False
        self.topic = settings.KAFKA_TOPIC_EMAIL_RECEIVED
        
        # 배치 프로세서 초기화
        self.orchestrator = EmailOrchestrator()
        self.batch_processor = EmailBatchProcessor(
            batch_size=10,  # 10개 이벤트마다 처리
            orchestrator=self.orchestrator,
            max_wait_time=5.0  # 5초 대기
        )
        
        # 주기적 플러시를 위한 태스크
        self.flush_task = None
    
    async def start(self):
        """컨슈머 시작"""
        logger.info(f"Starting Email Event Consumer V2 (Batch Mode) for topic: {self.topic}")
        
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-email-batch",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        await self.consumer.start()
        self._running = True
        
        # 주기적 플러시 태스크 시작
        self.flush_task = asyncio.create_task(self._periodic_flush())
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                
                try:
                    # 배치 프로세서에 이벤트 추가
                    await self.batch_processor.add_event(msg.value)
                    logger.debug(f"Added event to batch: {msg.value.get('event_id')}")
                    
                except Exception as e:
                    logger.error(f"Error adding event to batch: {str(e)}", exc_info=True)
                    
        finally:
            # 남은 이벤트 처리
            await self.batch_processor.flush()
            
            if self.flush_task:
                self.flush_task.cancel()
            
            await self.consumer.stop()
            logger.info("Email Event Consumer V2 stopped")
    
    async def _periodic_flush(self):
        """주기적으로 배치 플러시 (타임아웃 처리)"""
        while self._running:
            try:
                await asyncio.sleep(3.0)  # 3초마다 체크
                
                # 시간 초과된 배치가 있으면 처리
                if self.batch_processor.batch_start_time:
                    elapsed = asyncio.get_event_loop().time() - self.batch_processor.batch_start_time
                    if elapsed >= self.batch_processor.max_wait_time:
                        logger.info("Triggering batch processing due to timeout")
                        await self.batch_processor.flush()
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {str(e)}")
    
    async def stop(self):
        """컨슈머 중지"""
        logger.info("Stopping Email Event Consumer V2...")
        self._running = False
# infra/events/email_event_consumer.py
import logging
import json
import asyncio
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer
from datetime import datetime

from infra.core.config import settings
from content_email.email_batch_processor import EmailBatchProcessor
from content_email.orchestrator import EmailOrchestrator
from .event_logger import event_logger

logger = logging.getLogger(__name__)

class EmailEventConsumer:
    """이메일 이벤트 컨슈머 - 배치 처리 통합 버전"""
    
    def __init__(self):
        self.consumer = None
        self._running = False
        self.topic = settings.KAFKA_TOPIC_EMAIL_RECEIVED
        
        # 설정 검증
        self._validate_config()
        
        # 배치 프로세서 초기화
        self.orchestrator = EmailOrchestrator()
        self.batch_processor = EmailBatchProcessor(
            batch_size=settings.EMAIL_BATCH_SIZE,
            orchestrator=self.orchestrator,
            max_wait_time=settings.EMAIL_BATCH_MAX_WAIT_TIME
        )
        
        # 주기적 플러시를 위한 태스크
        self.flush_task = None
        
        # 통계
        self.stats = {
            'total_events': 0,
            'total_batches': 0,
            'failed_events': 0
        }
    
    def _validate_config(self):
        """설정 검증"""
        if settings.EMAIL_BATCH_SIZE <= 0 or settings.EMAIL_BATCH_SIZE > 100:
            raise ValueError(f"Invalid EMAIL_BATCH_SIZE: {settings.EMAIL_BATCH_SIZE}. Must be between 1 and 100.")
        
        if settings.EMAIL_MAX_CONCURRENT_BATCHES <= 0 or settings.EMAIL_MAX_CONCURRENT_BATCHES > 10:
            raise ValueError(f"Invalid EMAIL_MAX_CONCURRENT_BATCHES: {settings.EMAIL_MAX_CONCURRENT_BATCHES}. Must be between 1 and 10.")
        
        if settings.EMAIL_BATCH_MAX_WAIT_TIME <= 0 or settings.EMAIL_BATCH_MAX_WAIT_TIME > 60:
            raise ValueError(f"Invalid EMAIL_BATCH_MAX_WAIT_TIME: {settings.EMAIL_BATCH_MAX_WAIT_TIME}. Must be between 1 and 60 seconds.")
    
    async def start(self):
        """컨슈머 시작"""
        logger.info(f"Starting Email Event Consumer (Batch Mode)")
        logger.info(f"  - Topic: {self.topic}")
        logger.info(f"  - Batch size: {settings.EMAIL_BATCH_SIZE}")
        logger.info(f"  - Max wait time: {settings.EMAIL_BATCH_MAX_WAIT_TIME}s")
        logger.info(f"  - Max concurrent batches: {settings.EMAIL_MAX_CONCURRENT_BATCHES}")
        
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
                
                # 이벤트 로깅 시작
                log_id = None
                try:
                    # 이벤트 수신 로그
                    log_id = await event_logger.log_email_event_start(
                        topic=self.topic,
                        event_data=msg.value,
                        batch_mode=True
                    )
                    
                    # 배치 프로세서에 이벤트 추가
                    await self.batch_processor.add_event({
                        **msg.value,
                        '_log_id': log_id  # 로그 ID 전달
                    })
                    
                    self.stats['total_events'] += 1
                    logger.debug(f"Added event to batch: {msg.value.get('event_id')} (total: {self.stats['total_events']})")
                    
                except Exception as e:
                    self.stats['failed_events'] += 1
                    logger.error(f"Error adding event to batch: {str(e)}", exc_info=True)
                    
                    # 실패 로그
                    if log_id:
                        await event_logger.log_event_failed(log_id, str(e))
                    
        except Exception as e:
            logger.error(f"Consumer error: {str(e)}", exc_info=True)
            
        finally:
            # 종료 시 남은 이벤트 처리
            logger.info("Flushing remaining events before shutdown...")
            await self.batch_processor.flush()
            
            if self.flush_task:
                self.flush_task.cancel()
                try:
                    await self.flush_task
                except asyncio.CancelledError:
                    pass
            
            await self.consumer.stop()
            
            # 최종 통계 로그
            logger.info(f"Email Event Consumer stopped. Stats:")
            logger.info(f"  - Total events: {self.stats['total_events']}")
            logger.info(f"  - Total batches: {self.stats['total_batches']}")
            logger.info(f"  - Failed events: {self.stats['failed_events']}")
    
    async def _periodic_flush(self):
        """주기적으로 배치 플러시 (타임아웃 처리)"""
        flush_interval = settings.EMAIL_BATCH_FLUSH_INTERVAL  # 기본값: 3초
        
        while self._running:
            try:
                await asyncio.sleep(flush_interval)
                
                # 시간 초과된 배치가 있으면 처리
                async with self.batch_processor.lock:
                    if self.batch_processor.current_batch and self.batch_processor.batch_start_time:
                        elapsed = asyncio.get_event_loop().time() - self.batch_processor.batch_start_time
                        if elapsed >= self.batch_processor.max_wait_time:
                            logger.info(f"Triggering batch processing due to timeout (elapsed: {elapsed:.2f}s)")
                            await self.batch_processor._trigger_batch_processing()
                            self.stats['total_batches'] += 1
                        
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {str(e)}", exc_info=True)
    
    async def stop(self):
        """컨슈머 중지"""
        logger.info("Stopping Email Event Consumer...")
        self._running = False
    
    def get_stats(self) -> Dict[str, Any]:
        """현재 통계 반환"""
        return {
            **self.stats,
            'batch_processor_stats': self.batch_processor.get_stats() if hasattr(self.batch_processor, 'get_stats') else {}
        }

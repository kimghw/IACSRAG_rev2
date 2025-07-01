# infra/events/email_event_consumer.py
import logging
import json
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer
import asyncio

from infra.core.config import settings
from infra.events.handlers import handle_email_event
from .event_logger import event_logger

logger = logging.getLogger(__name__)

class EmailEventConsumer:
    """email.received 토픽을 구독하고 이메일 이벤트 처리 - 로깅 추가"""
    
    def __init__(self):
        self.consumer = None
        self._running = False
        self.topic = settings.KAFKA_TOPIC_EMAIL_RECEIVED
        
    async def start(self):
        """Email Event Consumer 시작"""
        logger.info(f"Starting Email Event Consumer for topic: {self.topic}")
        
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-email",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        await self.consumer.start()
        self._running = True
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                
                # 이벤트 처리 시작 로그
                log_id = None
                try:
                    # 이벤트 로깅 시작
                    log_id = await event_logger.log_event_start(self.topic, msg.value)
                    
                    # 실제 이벤트 처리
                    await handle_email_event(msg.value)
                    
                    # 성공 로그
                    await event_logger.log_event_complete(log_id)
                    
                except Exception as e:
                    logger.error(f"Error handling email event: {str(e)}", exc_info=True)
                    
                    # 실패 로그
                    if log_id:
                        await event_logger.log_event_failed(log_id, str(e))
                    
        finally:
            await self.consumer.stop()
            logger.info("Email Event Consumer stopped")
    
    async def stop(self):
        """Email Event Consumer 중지"""
        logger.info("Stopping Email Event Consumer...")
        self._running = False
# infra/events/email_event_consumer.py
import logging
import json
from typing import Dict, Any
from aiokafka import AIOKafkaConsumer
import asyncio

from infra.core.config import settings
from infra.events.handlers import handle_email_event

logger = logging.getLogger(__name__)

class EmailEventConsumer:
    """email.received 토픽을 구독하고 이메일 이벤트 처리"""
    
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
                
                try:
                    await handle_email_event(msg.value)
                except Exception as e:
                    logger.error(f"Error handling email event: {str(e)}", exc_info=True)
                    
        finally:
            await self.consumer.stop()
            logger.info("Email Event Consumer stopped")
    
    async def stop(self):
        """Email Event Consumer 중지"""
        logger.info("Stopping Email Event Consumer...")
        self._running = False
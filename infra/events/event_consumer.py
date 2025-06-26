# infra/events/event_consumer.py
from abc import ABC, abstractmethod
from typing import List, Dict, Any
import json
import asyncio
import logging
from aiokafka import AIOKafkaConsumer

from infra.core.config import settings

logger = logging.getLogger(__name__)

class EventConsumer(ABC):
    """이벤트 컨슈머 베이스 클래스"""
    
    def __init__(self, topics: List[str], group_id: str):
        self.topics = topics
        self.group_id = group_id
        self.consumer = None
        self._running = False
    
    async def start(self):
        """컨슈머 시작"""
        logger.info(f"Starting consumer for topics: {self.topics}, group_id: {self.group_id}")
        
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=self.group_id,
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
                    logger.debug(f"Received message: {msg.topic}:{msg.partition}:{msg.offset}")
                    await self.handle_message(msg.value)
                except Exception as e:
                    logger.error(f"Error handling message: {str(e)}")
                    # 여기에 DLQ 로직 추가 가능
                    
        finally:
            await self.consumer.stop()
            logger.info("Consumer stopped")
    
    async def stop(self):
        """컨슈머 중지"""
        logger.info("Stopping consumer...")
        self._running = False
    
    @abstractmethod
    async def handle_message(self, message: Dict[str, Any]):
        """메시지 처리 - 서브클래스에서 구현"""
        pass
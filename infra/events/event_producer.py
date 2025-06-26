import json
from typing import Dict, Any
from aiokafka import AIOKafkaProducer

from infra.core.config import settings

class EventProducer:
    """Kafka 이벤트 프로듀서"""
    
    def __init__(self):
        self.producer = None
    
    async def _ensure_producer(self):
        """프로듀서 초기화"""
        if self.producer is None:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode()
            )
            await self.producer.start()
    
    async def send_event(self, topic: str, key: str, value: Dict[str, Any]):
        """이벤트 발행"""
        await self._ensure_producer()
        
        await self.producer.send_and_wait(
            topic=topic,
            key=key.encode() if key else None,
            value=value
        )
    
    async def close(self):
        """프로듀서 종료"""
        if self.producer:
            await self.producer.stop()
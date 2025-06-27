# infra/events/event_producer.py
import json
import logging
from datetime import datetime
from typing import Dict, Any
from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from infra.core.config import settings

logger = logging.getLogger(__name__)

def json_serializer(obj):
    """JSON 직렬화 함수 - datetime 객체 처리"""
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

class EventProducer:
    """Kafka 이벤트 프로듀서 - 싱글톤 패턴"""
    
    _instance = None
    _producer = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # 싱글톤이므로 중복 초기화 방지
        pass
    
    async def _ensure_producer(self):
        """프로듀서 초기화 및 연결 확인"""
        if self._producer is None:
            logger.info("Initializing Kafka producer...")
            try:
                self._producer = AIOKafkaProducer(
                    bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v, default=json_serializer).encode(),
                    acks='all',  # 모든 replica 확인
                    enable_idempotence=True  # 중복 방지
                )
                await self._producer.start()
                logger.info("Kafka producer started successfully")
            except Exception as e:
                logger.error(f"Failed to start Kafka producer: {e}")
                self._producer = None
                raise
    
    async def send_event(self, topic: str, key: str, value: Dict[str, Any]):
        """이벤트 발행"""
        await self._ensure_producer()
        
        try:
            # 이벤트 발행
            logger.info(f"Sending event to topic '{topic}' with key '{key}'")
            logger.debug(f"Event data: {value}")
            
            # send_and_wait로 확실한 전송 보장
            record_metadata = await self._producer.send_and_wait(
                topic=topic,
                key=key.encode() if key else None,
                value=value
            )
            
            logger.info(f"Event sent successfully: {topic}:{record_metadata.partition}:{record_metadata.offset}")
            
        except KafkaError as e:
            logger.error(f"Kafka error while sending event: {e}")
            raise
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            raise
    
    async def close(self):
        """프로듀서 종료"""
        if self._producer:
            try:
                await self._producer.stop()
                logger.info("Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing producer: {e}")
            finally:
                self._producer = None
    
    @classmethod
    async def shutdown(cls):
        """클래스 레벨 shutdown - 애플리케이션 종료 시 호출"""
        if cls._instance and cls._instance._producer:
            await cls._instance.close()
            cls._instance = None

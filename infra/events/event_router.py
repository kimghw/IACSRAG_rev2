# infra/events/event_router.py
import logging
import json
from typing import Dict, Any, Callable
from aiokafka import AIOKafkaConsumer
import asyncio

from infra.core.config import settings
from infra.databases.mongo_db import MongoDB
from schema import DocumentEventType
from .event_logger import event_logger

logger = logging.getLogger(__name__)

class EventRouter:
    """통합 이벤트 라우터 - 모든 이벤트를 적절한 핸들러로 라우팅"""
    
    def __init__(self):
        self.handlers: Dict[str, Callable] = {}
        self.consumer = None
        self._running = False
        self.topics = []
        self.mongo = MongoDB()
        
    def register_handler(self, event_key: str, handler: Callable):
        """이벤트 핸들러 등록 - 토픽명 또는 이벤트 타입을 키로 사용"""
        self.handlers[event_key] = handler
        logger.info(f"Registered handler for event: {event_key}")
    
    def add_topic(self, topic: str):
        """구독할 토픽 추가"""
        if topic not in self.topics:
            self.topics.append(topic)
            logger.info(f"Added topic: {topic}")
    
    async def start(self):
        """Event Router 시작"""
        if not self.topics:
            logger.error("No topics to subscribe")
            return
            
        logger.info(f"Starting Event Router for topics: {self.topics}")
        
        self.consumer = AIOKafkaConsumer(
            *self.topics,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-event-router",
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
                    await self._route_event(msg.topic, msg.value)
                except Exception as e:
                    logger.error(f"Error routing event: {str(e)}", exc_info=True)
                    
        finally:
            await self.consumer.stop()
            logger.info("Event Router stopped")
    
    async def _route_event(self, topic: str, event_data: Dict[str, Any]):
        """이벤트를 적절한 핸들러로 라우팅"""
        # 이벤트 로깅 시작
        log_id = await event_logger.log_event_start(topic, event_data)
        
        try:
            # 1. 토픽 기반 라우팅 시도
            handler = self.handlers.get(topic)
            
            # 2. document.uploaded 토픽의 경우 event_type 기반 라우팅
            if not handler and topic == settings.KAFKA_TOPIC_DOCUMENT_UPLOADED:
                event_type_str = event_data.get('event_type')
                if event_type_str:
                    try:
                        event_type = DocumentEventType(event_type_str)
                        handler = self.handlers.get(event_type)
                    except ValueError:
                        logger.error(f"Unknown document event type: {event_type_str}")
            
            # 3. 핸들러 실행
            if handler:
                # 중복 체크 (문서 이벤트의 경우)
                if topic == settings.KAFKA_TOPIC_DOCUMENT_UPLOADED:
                    document_id = event_data.get('document_id')
                    if document_id and await self._is_already_processed(document_id):
                        logger.info(f"Document {document_id} already processed, skipping")
                        await event_logger.log_event_complete(log_id)
                        return
                
                # 이메일 이벤트의 경우 중복 체크
                elif topic == settings.KAFKA_TOPIC_EMAIL_RECEIVED:
                    event_id = event_data.get('event_id')
                    if event_id and await self._is_email_already_processed(event_id):
                        logger.info(f"Email event {event_id} already processed, skipping")
                        await event_logger.log_event_complete(log_id)
                        return
                
                logger.info(f"Routing {topic} event: {event_data.get('event_id') or event_data.get('document_id')}")
                await handler(event_data)
                logger.info(f"Successfully handled event")
                
                # 성공 로그
                await event_logger.log_event_complete(log_id)
                
            else:
                error_msg = f"No handler registered for topic: {topic}"
                logger.warning(error_msg)
                await event_logger.log_event_failed(log_id, error_msg)
                
        except Exception as e:
            error_msg = f"Handler failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            await event_logger.log_event_failed(log_id, error_msg)
    
    async def _is_already_processed(self, document_id: str) -> bool:
        """문서가 이미 처리되었는지 확인"""
        try:
            # uploads 컬렉션에서 확인
            existing_upload = await self.mongo.db.uploads.find_one({'document_id': document_id})
            if existing_upload and existing_upload.get('status') == 'completed':
                return True
            
            # pdf_chunks 컬렉션에서도 확인
            existing_chunk = await self.mongo.db.pdf_chunks.find_one({'document_id': document_id})
            if existing_chunk:
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error checking if document is processed: {str(e)}")
            return False
    
    async def _is_email_already_processed(self, event_id: str) -> bool:
        """이메일 이벤트가 이미 처리되었는지 확인"""
        try:
            # event_logs에서 확인
            existing = await self.mongo.db.event_logs.find_one({
                'event_id': event_id,
                'status': 'completed'
            })
            return existing is not None
            
        except Exception as e:
            logger.error(f"Error checking if email event is processed: {str(e)}")
            return False
    
    async def stop(self):
        """Event Router 중지"""
        logger.info("Stopping Event Router...")
        self._running = False


class DocumentEventRouter(EventRouter):
    """문서 이벤트 전용 라우터 (하위 호환성)"""
    
    def __init__(self):
        super().__init__()
        self.topic = settings.KAFKA_TOPIC_DOCUMENT_UPLOADED
        self.add_topic(self.topic)
    
    def register_handler(self, event_type: DocumentEventType, handler: Callable):
        """문서 이벤트 타입별 핸들러 등록"""
        super().register_handler(event_type, handler)


class EmailEventRouter(EventRouter):
    """이메일 이벤트 전용 라우터"""
    
    def __init__(self):
        super().__init__()
        self.topic = settings.KAFKA_TOPIC_EMAIL_RECEIVED
        self.add_topic(self.topic)
# infra/events/document_event_router.py
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

class DocumentEventRouter:
    """document.uploaded 토픽을 구독하고 이벤트 타입에 따라 적절한 핸들러로 라우팅"""
    
    def __init__(self):
        self.handlers: Dict[DocumentEventType, Callable] = {}
        self.consumer = None
        self._running = False
        self.topic = settings.KAFKA_TOPIC_DOCUMENT_UPLOADED
        self.mongo = MongoDB()  # MongoDB 인스턴스 추가
        
    def register_handler(self, event_type: DocumentEventType, handler: Callable):
        """이벤트 타입별 핸들러 등록"""
        self.handlers[event_type] = handler
        logger.info(f"Registered handler for document event type: {event_type.value}")
    
    async def start(self):
        """Document Event Router 시작"""
        logger.info(f"Starting Document Event Router for topic: {self.topic}")
        
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-document-router",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True  # 일단 자동 커밋 유지
        )
        
        await self.consumer.start()
        self._running = True
        
        try:
            async for msg in self.consumer:
                if not self._running:
                    break
                
                try:
                    await self._route_event(msg.value)
                except Exception as e:
                    logger.error(f"Error routing event: {str(e)}", exc_info=True)
                    
        finally:
            await self.consumer.stop()
            logger.info("Document Event Router stopped")
    
    async def _is_already_processed(self, document_id: str) -> bool:
        """문서가 이미 처리되었는지 확인"""
        try:
            # uploads 컬렉션에서 확인 (모든 문서 타입이 여기 저장됨)
            existing_upload = await self.mongo.db.uploads.find_one({'document_id': document_id})
            if existing_upload and existing_upload.get('status') == 'completed':
                return True
            
            # pdf_chunks 컬렉션에서도 확인 (PDF/Markdown의 경우)
            existing_chunk = await self.mongo.db.pdf_chunks.find_one({'document_id': document_id})
            if existing_chunk:
                return True
                
            return False
            
        except Exception as e:
            logger.error(f"Error checking if document is processed: {str(e)}")
            return False
    
    async def _route_event(self, event_data: Dict[str, Any]):
        """문서 이벤트를 적절한 핸들러로 라우팅 - 로깅 및 중복 체크 추가"""
        event_type_str = event_data.get('event_type')
        document_id = event_data.get('document_id')
        
        # 이벤트 처리 시작 로그
        log_id = await event_logger.log_event_start(self.topic, event_data)
        
        if not event_type_str:
            error_msg = f"Event type missing in document event: {event_data}"
            logger.error(error_msg)
            await event_logger.log_event_failed(log_id, error_msg)
            return
        
        if not document_id:
            error_msg = f"Document ID missing in document event: {event_data}"
            logger.error(error_msg)
            await event_logger.log_event_failed(log_id, error_msg)
            return
        
        # 중복 처리 체크
        if await self._is_already_processed(document_id):
            logger.info(f"Document {document_id} already processed, skipping")
            await event_logger.log_event_complete(log_id)  # 이미 처리된 것도 성공으로 기록
            return
        
        try:
            event_type = DocumentEventType(event_type_str)
        except ValueError:
            error_msg = f"Unknown document event type: {event_type_str}"
            logger.error(error_msg)
            await event_logger.log_event_failed(log_id, error_msg)
            return
        
        handler = self.handlers.get(event_type)
        
        if handler:
            logger.info(f"Routing {event_type.value} event for document: {document_id}")
            try:
                await handler(event_data)
                logger.info(f"Successfully handled {event_type.value} event")
                
                # 성공 로그
                await event_logger.log_event_complete(log_id)
                
            except Exception as e:
                error_msg = f"Handler failed for {event_type.value}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                
                # 실패 로그
                await event_logger.log_event_failed(log_id, error_msg)
        else:
            error_msg = f"No handler registered for event type: {event_type.value}"
            logger.warning(error_msg)
            await event_logger.log_event_failed(log_id, error_msg)
    
    async def stop(self):
        """Document Event Router 중지"""
        logger.info("Stopping Document Event Router...")
        self._running = False
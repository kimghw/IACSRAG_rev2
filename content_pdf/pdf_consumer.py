# content_pdf/pdf_consumer.py
from typing import Dict, Any, Set
import asyncio
import logging

from .orchestrator import PdfOrchestrator
from .schema import PdfProcessingRequest
from infra.events.event_consumer import EventConsumer
from infra.core.config import settings

logger = logging.getLogger(__name__)

class PdfConsumer(EventConsumer):
    def __init__(self):
        super().__init__(
            topics=[settings.KAFKA_TOPIC_DOCUMENT_UPLOADED],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-pdf"
        )
        self.orchestrator = PdfOrchestrator()
        self.processing_tasks: Set[asyncio.Task] = set()
        self.max_concurrent = settings.PDF_MAX_CONCURRENT_PROCESSING
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
    
    async def start(self):
        """컨슈머 시작 - 동시 처리 지원"""
        logger.info(f"Starting PDF consumer with max {self.max_concurrent} concurrent tasks")
        
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
                
                # 태스크 생성하여 동시 처리
                task = asyncio.create_task(self._process_message_with_limit(msg.value))
                self.processing_tasks.add(task)
                
                # 완료된 태스크 정리
                self.processing_tasks = {t for t in self.processing_tasks if not t.done()}
                
                # 동시 처리 수 로깅
                if len(self.processing_tasks) > 1:
                    logger.info(f"Currently processing {len(self.processing_tasks)} PDFs concurrently")
                    
        finally:
            # 모든 진행 중인 태스크 대기
            if self.processing_tasks:
                logger.info(f"Waiting for {len(self.processing_tasks)} tasks to complete...")
                await asyncio.gather(*self.processing_tasks, return_exceptions=True)
                
            await self.consumer.stop()
            logger.info("PDF Consumer stopped")
    
    async def _process_message_with_limit(self, message: Dict[str, Any]):
        """세마포어를 사용한 동시 처리 제한"""
        async with self.semaphore:
            try:
                await self.handle_message(message)
            except Exception as e:
                logger.error(f"Error processing message: {str(e)}", exc_info=True)
    
    async def handle_message(self, message: Dict[str, Any]):
        """메시지 처리"""
        # content_type이 PDF인 경우만 처리
        if message.get("content_type") != "application/pdf":
            return
        
        document_id = message.get("document_id")
        logger.info(f"Starting PDF processing for document: {document_id}")
        
        # PDF 처리 요청 생성 (file_path 없이)
        request = PdfProcessingRequest(
            document_id=document_id,
            metadata=message.get("metadata", {})
        )
        
        # Orchestrator를 통해 처리
        await self.orchestrator.process_pdf(request)
        
        logger.info(f"Completed PDF processing for document: {document_id}")
from typing import Dict, Any

from .orchestrator import PdfOrchestrator
from schema import PdfProcessingRequest
from infra.events.event_consumer import EventConsumer
from infra.core.config import settings

class PdfConsumer(EventConsumer):
    """PDF 문서 처리 컨슈머"""
    
    def __init__(self):
        super().__init__(
            topics=[settings.KAFKA_TOPIC_DOCUMENT_UPLOADED],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-pdf"
        )
        self.orchestrator = PdfOrchestrator()
    
    # content_pdf/pdf_consumer.py
async def handle_message(self, message: Dict[str, Any]):
    if message.get("content_type") != "application/pdf":
        return
    
    # file_path 대신 document_id 사용
    request = PdfProcessingRequest(
        document_id=message["document_id"],
        # file_path=message["file_path"],  # 제거
        metadata=message.get("metadata", {})
    )
        
    # Orchestrator를 통해 처리
    await self.orchestrator.process_pdf(request)
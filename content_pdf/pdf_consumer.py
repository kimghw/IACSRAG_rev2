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
    
    async def handle_message(self, message: Dict[str, Any]):
        """document.uploaded 이벤트 처리"""
        
        # PDF 타입만 처리
        if message.get("content_type") != "application/pdf":
            return
        
        # PDF 처리 요청 생성
        request = PdfProcessingRequest(
            document_id=message["document_id"],
            file_path=message["file_path"],
            metadata=message.get("metadata", {})
        )
        
        # Orchestrator를 통해 처리
        await self.orchestrator.process_pdf(request)
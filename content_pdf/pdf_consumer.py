# content_pdf/pdf_consumer.py
from .orchestrator import PdfOrchestrator
from schema import PdfProcessingRequest
from infra.events.event_consumer import EventConsumer

class PdfConsumer(EventConsumer):
    def __init__(self):
        super().__init__(
            topics=[settings.KAFKA_TOPIC_DOCUMENT_UPLOADED],
            group_id=f"{settings.KAFKA_CONSUMER_GROUP_ID}-pdf"
        )
        self.orchestrator = PdfOrchestrator()
    
    async def handle_message(self, message: Dict[str, Any]):
        if message.get("content_type") != "application/pdf":
            return
        
        # file_path 대신 document_id만 사용
        request = PdfProcessingRequest(
            document_id=message["document_id"],
            metadata=message.get("metadata", {})
        )
        
        await self.orchestrator.process_pdf(request)
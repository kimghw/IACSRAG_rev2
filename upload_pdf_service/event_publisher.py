from infra.events.event_producer import EventProducer
from infra.core.config import settings
from schema import DocumentUploadedEvent

class EventPublisher:
    """업로드 이벤트 발행자"""
    
    def __init__(self):
        self.producer = EventProducer()
    
    async def publish_document_uploaded(self, event: DocumentUploadedEvent):
        """document.uploaded 이벤트 발행"""
        await self.producer.send_event(
            topic=settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            key=event.document_id,
            value=event.dict()
        )
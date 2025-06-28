# upload_service/event_publisher.py
from infra.events.event_producer import EventProducer
from infra.core.config import settings
from schema import DocumentUploadedEvent

class EventPublisher:
    """업로드 이벤트 발행자"""
    
    def __init__(self):
        self.producer = EventProducer()
    
    async def publish_document_uploaded(self, event: DocumentUploadedEvent):
        """document.uploaded 이벤트 발행"""
        # event_type이 enum이므로 .value로 변환
        event_dict = event.dict()
        event_dict['event_type'] = event.event_type.value  # Enum을 string으로
        
        await self.producer.send_event(
            topic=settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            key=event.document_id,
            value=event_dict
        )
# infra/events/handlers/content_email_handler.py
import logging
from typing import Dict, Any
from content_email.orchestrator import EmailOrchestrator
from content_email.schema import EmailProcessingRequest

logger = logging.getLogger(__name__)

async def handle_email_event(event_data: Dict[str, Any]) -> None:
    """이메일 처리 이벤트 핸들러"""
    event_id = event_data.get('event_id')
    account_id = event_data.get('account_id')
    
    logger.info(f"📧 Processing email event: {event_id}")
    logger.info(f"   - Account ID: {account_id}")
    logger.info(f"   - Occurred at: {event_data.get('occurred_at')}")
    
    try:
        # Email 모듈의 EmailOrchestrator를 사용하여 실제 처리
        orchestrator = EmailOrchestrator()
        request = EmailProcessingRequest(
            event_id=event_id,
            account_id=account_id,
            event_data=event_data
        )
        
        # 실제 처리 수행
        result = await orchestrator.process_email(request)
        
        logger.info(f"✅ Email processing completed: {event_id}")
        logger.info(f"   - Emails processed: {result.email_count}")
        logger.info(f"   - Total attachments: {result.total_attachments}")
        logger.info(f"   - Downloaded attachments: {result.downloaded_attachments}")
        logger.info(f"   - Processing time: {result.processing_time:.2f}s")
        
    except Exception as e:
        logger.error(f"❌ Email processing failed for {event_id}: {str(e)}", exc_info=True)
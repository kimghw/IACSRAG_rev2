# infra/events/handlers/content_md_handler.py
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)

async def handle_markdown_event(event_data: Dict[str, Any]) -> None:
    """Markdown 문서 처리 이벤트 핸들러"""
    document_id = event_data.get('document_id')
    logger.info(f"📝 Processing Markdown: {document_id}")
    
    try:
        # TODO: Markdown 처리 로직 구현
        logger.info(f"✅ Markdown processing completed: {document_id}")
        
    except Exception as e:
        logger.error(f"❌ Markdown processing failed for {document_id}: {str(e)}", exc_info=True)
# infra/events/handlers/content_md_handler.py
import logging
from typing import Dict, Any
from content_pdf.orchestrator import ContentOrchestrator
from content_pdf.schema import ProcessingRequest

logger = logging.getLogger(__name__)

async def handle_markdown_event(event_data: Dict[str, Any]) -> None:
    """Markdown 문서 처리 이벤트 핸들러 - PDF 모듈의 ContentOrchestrator 사용"""
    document_id = event_data.get('document_id')
    logger.info(f"📝 Processing Markdown: {document_id}")
    
    try:
        # PDF 모듈의 ContentOrchestrator를 사용하여 실제 처리
        orchestrator = ContentOrchestrator()
        request = ProcessingRequest(
            document_id=document_id,
            metadata=event_data.get('metadata', {})
        )
        
        # 실제 처리 수행
        result = await orchestrator.process_content(request)
        
        logger.info(f"✅ Markdown processing completed: {document_id}")
        logger.info(f"   - Content type: {result.original_type}")
        logger.info(f"   - Chunks: {result.processed_data.get('total_chunks', 0)}")
        logger.info(f"   - Embeddings: {result.processed_data.get('embeddings_created', 0)}")
        
    except Exception as e:
        logger.error(f"❌ Markdown processing failed for {document_id}: {str(e)}", exc_info=True)
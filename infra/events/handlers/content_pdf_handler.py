# infra/events/handlers/content_pdf_handler.py
import logging
from typing import Dict, Any
from content_pdf.orchestrator import ContentOrchestrator  # 이름 변경
from content_pdf.schema import ProcessingRequest

logger = logging.getLogger(__name__)

async def handle_pdf_event(event_data: Dict[str, Any]) -> None:
    """PDF 문서 처리 이벤트 핸들러"""
    document_id = event_data.get('document_id')
    logger.info(f"📄 Processing PDF: {document_id}")
    
    try:
        orchestrator = ContentOrchestrator()
        request = ProcessingRequest(
            document_id=document_id,
            metadata=event_data.get('metadata', {})
        )
        
        result = await orchestrator.process_content(request)
        
        logger.info(f"✅ PDF processing completed: {document_id}")
        logger.info(f"   - Content type: {result.original_type}")
        logger.info(f"   - Chunks: {result.processed_data.get('total_chunks', 0)}")
        logger.info(f"   - Embeddings: {result.processed_data.get('embeddings_created', 0)}")
        
    except Exception as e:
        logger.error(f"❌ PDF processing failed for {document_id}: {str(e)}", exc_info=True)

async def handle_markdown_event(event_data: Dict[str, Any]) -> None:
    """Markdown 문서 처리 이벤트 핸들러 - PDF 모듈 재사용"""
    document_id = event_data.get('document_id')
    logger.info(f"📝 Processing Markdown: {document_id}")
    
    try:
        # PDF 모듈의 ContentOrchestrator를 재사용
        orchestrator = ContentOrchestrator()
        request = ProcessingRequest(
            document_id=document_id,
            metadata=event_data.get('metadata', {})
        )
        
        result = await orchestrator.process_content(request)
        
        logger.info(f"✅ Markdown processing completed: {document_id}")
        logger.info(f"   - Content type: {result.original_type}")
        logger.info(f"   - Chunks: {result.processed_data.get('total_chunks', 0)}")
        logger.info(f"   - Embeddings: {result.processed_data.get('embeddings_created', 0)}")
        
    except Exception as e:
        logger.error(f"❌ Markdown processing failed for {document_id}: {str(e)}", exc_info=True)
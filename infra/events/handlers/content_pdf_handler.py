# infra/events/handlers/pdf_handler.py
import logging
from typing import Dict, Any
from content_pdf.orchestrator import PdfOrchestrator
from content_pdf.schema import PdfProcessingRequest

logger = logging.getLogger(__name__)

async def handle_pdf_event(event_data: Dict[str, Any]) -> None:
    """PDF 문서 처리 이벤트 핸들러"""
    document_id = event_data.get('document_id')
    logger.info(f"📄 Processing PDF: {document_id}")
    
    try:
        orchestrator = PdfOrchestrator()
        request = PdfProcessingRequest(
            document_id=document_id,
            metadata=event_data.get('metadata', {})
        )
        
        result = await orchestrator.process_pdf(request)
        
        logger.info(f"✅ PDF processing completed: {document_id}")
        logger.info(f"   - Chunks: {result.processed_data.get('chunks', 0)}")
        logger.info(f"   - Embeddings: {result.processed_data.get('embeddings', 0)}")
        
    except Exception as e:
        logger.error(f"❌ PDF processing failed for {document_id}: {str(e)}", exc_info=True)


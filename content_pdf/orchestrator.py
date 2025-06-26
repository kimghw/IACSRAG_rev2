# content_pdf/orchestrator.py
from typing import Dict, Any, List
import logging
from datetime import datetime, timezone

from .pdf_processor import PdfProcessor
from .pdf_chunker import PdfChunker
from .pdf_embedder import PdfEmbedder
from .repository import PdfRepository
from schema import PdfProcessingRequest, ProcessedContent, ChunkData, EmbeddingData

logger = logging.getLogger(__name__)

class PdfOrchestrator:
    """PDF 처리 오케스트레이터 - 명확한 단계별 프로세스"""
    
    def __init__(self):
        self.processor = PdfProcessor()
        self.chunker = PdfChunker()
        self.embedder = PdfEmbedder()
        self.repository = PdfRepository()
    
    async def process_pdf(self, request: PdfProcessingRequest) -> ProcessedContent:
        """
        PDF 처리 메인 플로우
        1. 텍스트 추출
        2. 청킹
        3. 임베딩 생성
        4. Qdrant 저장
        """
        try:
            logger.info(f"Starting PDF processing for document: {request.document_id}")
            
            # 1. 텍스트 추출 (document_id만 전달)
            extracted_text = await self.processor.extract_text(request.document_id)
            logger.info(f"Extracted {len(extracted_text)} characters from PDF")
            
            # 2. 청킹
            chunks = await self.chunker.create_chunks(
                document_id=request.document_id,
                text=extracted_text,
                metadata=request.metadata
            )
            logger.info(f"Created {len(chunks)} chunks")
            
            # 3. 임베딩 생성
            embeddings = await self.embedder.generate_embeddings(chunks)
            logger.info(f"Generated {len(embeddings)} embeddings")
            
            # 4. Qdrant 저장
            await self.repository.save_to_qdrant(embeddings)
            logger.info(f"Saved embeddings to Qdrant")
            
            # 5. 처리 상태 업데이트
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="completed"
            )
            
            return ProcessedContent(
                content_id=request.document_id,
                original_type="pdf",
                processed_data={"chunks": len(chunks), "embeddings": len(embeddings)},
                embeddings=[{"id": e.embedding_id, "text": e.embedding_text[:100]} for e in embeddings],
                metadata=request.metadata,
                processed_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            logger.error(f"PDF processing failed: {str(e)}")
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="failed",
                error_message=str(e)
            )
            raise
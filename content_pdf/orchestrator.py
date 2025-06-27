# content_pdf/orchestrator.py
from typing import Dict, Any, List
import logging
import time
from datetime import datetime, timezone
import gc

from .pdf_processor import PdfProcessor
from .pdf_chunker import PdfChunker
from .pdf_embedder import PdfEmbedder
from .repository import PdfRepository
from .schema import PdfProcessingRequest, ChunkDocument  # 모듈 내부 스키마
from schema import ProcessedContent  # 루트 공유 스키마
from infra.core.config import settings
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class PdfOrchestrator:
    """PDF 처리 오케스트레이터 - 배치 처리 및 메모리 관리"""
    
    def __init__(self):
        self.processor = PdfProcessor()
        self.chunker = PdfChunker()
        self.embedder = PdfEmbedder()
        self.repository = PdfRepository()
        self.batch_size = settings.PDF_BATCH_SIZE
    
    async def process_pdf(self, request: PdfProcessingRequest) -> ProcessedContent:
        """
        PDF 처리 메인 플로우 with 배치 처리
        1. MongoDB에서 PDF를 메모리로 읽기
        2. 텍스트 추출 및 청킹
        3. 배치 단위 처리:
           - 임베딩 생성
           - MongoDB에 청크 저장
           - 인덱싱 정보 업데이트
           - Qdrant에 임베딩 저장
           - 배치 메모리 해제
        4. 전체 처리 완료 후 PDF 메모리 해제
        """
        file_content = None
        total_chunks_processed = 0
        total_embeddings_created = 0
        total_start_time = time.time()
        
        try:
            logger.info(f"Starting PDF processing for document: {request.document_id}")
            
            # 1. 텍스트 추출
            text_start_time = time.time()
            processing_logger.text_extraction_started(request.document_id)
            
            extracted_text, file_content = await self.processor.extract_text_to_memory(
                request.document_id
            )
            
            text_duration = time.time() - text_start_time
            processing_logger.text_extraction_completed(
                document_id=request.document_id,
                duration=text_duration,
                text_length=len(extracted_text)
            )
            
            # 2. 청킹
            chunk_start_time = time.time()
            processing_logger.chunking_started(request.document_id)
            
            chunk_documents = await self.chunker.create_chunks(
                document_id=request.document_id,
                text=extracted_text,
                metadata=request.metadata
            )
            
            chunk_duration = time.time() - chunk_start_time
            processing_logger.chunking_completed(
                document_id=request.document_id,
                duration=chunk_duration,
                chunk_count=len(chunk_documents)
            )
            
            # 텍스트는 더 이상 필요없으므로 메모리에서 해제
            del extracted_text
            
            # 3. 임베딩 생성 시작
            embedding_start_time = time.time()
            processing_logger.embedding_started(
                document_id=request.document_id,
                chunk_count=len(chunk_documents),
                model=settings.OPENAI_EMBEDDING_MODEL
            )
            
            # 4. 배치 단위 처리
            for batch_idx in range(0, len(chunk_documents), self.batch_size):
                batch_end = min(batch_idx + self.batch_size, len(chunk_documents))
                chunk_batch = chunk_documents[batch_idx:batch_end]
                
                logger.info(f"Processing batch {batch_idx//self.batch_size + 1} "
                          f"(chunks {batch_idx}-{batch_end-1})")
                
                # a) 배치 임베딩 생성
                embeddings, index_info_map = await self.embedder.generate_batch_embeddings(
                    chunk_batch
                )
                
                # b) MongoDB에 청크 저장 (비동기)
                await self.repository.save_chunks_to_mongo(chunk_batch)
                
                # c) 인덱싱 정보 업데이트
                for chunk_id, index_info in index_info_map.items():
                    await self.repository.update_chunk_index_info(chunk_id, index_info)
                
                # d) Qdrant에 임베딩 저장
                await self.repository.save_embeddings_to_qdrant(embeddings)
                
                # 통계 업데이트
                total_chunks_processed += len(chunk_batch)
                total_embeddings_created += len(embeddings)
                
                # e) 배치 메모리 해제
                del chunk_batch
                del embeddings
                del index_info_map
                gc.collect()  # 명시적 가비지 컬렉션
                
                logger.info(f"Batch {batch_idx//self.batch_size + 1} completed and memory freed")
            
            # 임베딩 생성 완료
            embedding_duration = time.time() - embedding_start_time
            processing_logger.embedding_completed(
                document_id=request.document_id,
                duration=embedding_duration,
                embedding_count=total_embeddings_created
            )
            
            # 5. 벡터 저장 (이미 배치에서 완료되었지만 로그용)
            processing_logger.vector_storage_completed(
                document_id=request.document_id,
                duration=0.0,  # 배치에서 이미 처리됨
                stored_count=total_embeddings_created
            )
            
            # 6. 전체 청크 리스트 메모리 해제
            del chunk_documents
            
            # 7. 처리 상태 업데이트
            processed_info = {
                "total_chunks": total_chunks_processed,
                "total_embeddings": total_embeddings_created,
                "batch_size": self.batch_size
            }
            
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="completed",
                processed_info=processed_info
            )
            
            # 8. 전체 처리 완료 로그
            total_duration = time.time() - total_start_time
            processing_logger.processing_completed(
                document_id=request.document_id,
                total_duration=total_duration,
                stats={
                    "chunks": total_chunks_processed,
                    "embeddings": total_embeddings_created,
                    "batches": (total_chunks_processed + self.batch_size - 1) // self.batch_size,
                    "text_extraction_time": f"{text_duration:.2f}s",
                    "chunking_time": f"{chunk_duration:.2f}s",
                    "embedding_time": f"{embedding_duration:.2f}s"
                }
            )
            
            return ProcessedContent(
                content_id=request.document_id,
                original_type="pdf",
                processed_data={
                    "chunks": total_chunks_processed,
                    "embeddings": total_embeddings_created,
                    "batches_processed": (total_chunks_processed + self.batch_size - 1) // self.batch_size
                },
                metadata=request.metadata,
                processed_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            processing_logger.error(
                document_id=request.document_id,
                step="pdf_processing",
                error=str(e)
            )
            logger.error(f"PDF processing failed: {str(e)}")
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="failed",
                error_message=str(e)
            )
            raise
        
        finally:
            # PDF 파일 내용 메모리에서 완전 해제
            if file_content:
                del file_content
                gc.collect()
                logger.info("PDF file content removed from memory")

# content_pdf/services/batch_processor.py
import asyncio
from typing import List, Dict, Any
import logging

from ..schema import ChunkDocument
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class BatchProcessor:
    """청크 배치 처리기"""
    
    def __init__(self, batch_size: int, embedding_service, storage_service, document_id: str):
        self.batch_size = batch_size
        self.embedding_service = embedding_service
        self.storage_service = storage_service
        self.document_id = document_id
        self.current_batch = []
        self.stats = {
            'batches_processed': 0,
            'chunks_saved': 0,
            'embeddings_created': 0,
            'storage_errors': 0,
            'embedding_errors': 0
        }
        
    async def add_chunk(self, chunk: ChunkDocument):
        """청크를 배치에 추가"""
        self.current_batch.append(chunk)
        
        if len(self.current_batch) >= self.batch_size:
            await self._process_batch()
    
    async def flush(self):
        """남은 청크 처리"""
        if self.current_batch:
            await self._process_batch()
        return self.stats
    
    async def _process_batch(self):
        """배치 처리 - 임베딩 + 저장"""
        batch_number = self.stats['batches_processed'] + 1
        batch_size = len(self.current_batch)
        
        logger.info(f"Processing batch {batch_number} with {batch_size} chunks")
        
        try:
            # 1. MongoDB에 청크 저장
            chunk_ids = await self.storage_service.save_chunks(self.current_batch)
            self.stats['chunks_saved'] += len(chunk_ids)
            
            # 2. 멀티플렉싱으로 임베딩 생성
            embedding_start = asyncio.get_event_loop().time()  # 시간 측정 시작
            
            processing_logger.embedding_started(
                document_id=self.document_id,
                chunk_count=batch_size,
                model=self.embedding_service.model
            )
            
            embeddings = await self.embedding_service.generate_embeddings_batch(
                self.current_batch
            )
            
            embedding_duration = asyncio.get_event_loop().time() - embedding_start  # 시간 계산
            
            if embeddings:
                # 3. Qdrant에 벡터 저장
                await self.storage_service.save_embeddings(embeddings, self.current_batch)
                self.stats['embeddings_created'] += len(embeddings)
                
                processing_logger.embedding_completed(
                    document_id=self.document_id,
                    duration=embedding_duration,  # 실제 시간 전달
                    embedding_count=len(embeddings)
                )
            else:
                self.stats['embedding_errors'] += 1
                logger.error(f"No embeddings generated for batch {batch_number}")
            
            # 통계 업데이트
            self.stats['batches_processed'] += 1
            
        except Exception as e:
            self.stats['storage_errors'] += 1
            logger.error(f"Batch {batch_number} processing failed: {e}")
            raise
        finally:
            self.current_batch = []
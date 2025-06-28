# content_pdf/services/batch_processor.py
import asyncio
from typing import List, Dict, Any
import logging

from ..schema import ChunkDocument
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

class BatchProcessor:
    """청크 배치 처리기 - 비동기 처리 추가"""
    
    def __init__(self, batch_size: int, embedding_service, storage_service, document_id: str):
        self.batch_size = batch_size
        self.embedding_service = embedding_service
        self.storage_service = storage_service
        self.document_id = document_id
        self.current_batch = []
        self.background_tasks = []  # 백그라운드 태스크 추적
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
            # 배치를 백그라운드로 처리 (await 없음!)
            batch_to_process = self.current_batch[:]
            self.current_batch = []
            
            # 백그라운드 태스크 생성
            task = asyncio.create_task(
                self._process_batch_background(batch_to_process)
            )
            self.background_tasks.append(task)
    
    async def flush(self):
        """남은 청크 처리 및 모든 백그라운드 태스크 완료 대기"""
        # 남은 청크 처리
        if self.current_batch:
            task = asyncio.create_task(
                self._process_batch_background(self.current_batch[:])
            )
            self.background_tasks.append(task)
            self.current_batch = []
        
        # 모든 백그라운드 태스크 완료 대기
        if self.background_tasks:
            logger.info(f"Waiting for {len(self.background_tasks)} background tasks...")
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        
        return self.stats
    
    async def _process_batch_background(self, batch: List[ChunkDocument]):
        """배치를 백그라운드에서 처리"""
        batch_number = self.stats['batches_processed'] + 1
        self.stats['batches_processed'] += 1
        batch_size = len(batch)
        
        logger.info(f"Processing batch {batch_number} with {batch_size} chunks in background")
        
        try:
            # 1. MongoDB에 청크 저장
            chunk_ids = await self.storage_service.save_chunks(batch)
            self.stats['chunks_saved'] += len(chunk_ids)
            
            # 2. 멀티플렉싱으로 임베딩 생성
            embedding_start = asyncio.get_event_loop().time()
            
            processing_logger.embedding_started(
                document_id=self.document_id,
                chunk_count=batch_size,
                model=self.embedding_service.model
            )
            
            embeddings = await self.embedding_service.generate_embeddings_batch(batch)
            
            embedding_duration = asyncio.get_event_loop().time() - embedding_start
            
            if embeddings:
                # 3. Qdrant에 벡터 저장
                await self.storage_service.save_embeddings(embeddings, batch)
                self.stats['embeddings_created'] += len(embeddings)
                
                processing_logger.embedding_completed(
                    document_id=self.document_id,
                    duration=embedding_duration,
                    embedding_count=len(embeddings)
                )
            else:
                self.stats['embedding_errors'] += 1
                logger.error(f"No embeddings generated for batch {batch_number}")
            
            logger.info(f"Batch {batch_number} completed successfully")
            
        except Exception as e:
            self.stats['storage_errors'] += 1
            logger.error(f"Batch {batch_number} processing failed: {e}")
            # 에러는 로그만 하고 전파하지 않음 (다른 배치 처리 계속)
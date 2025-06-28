# content_pdf/orchestrator.py
import asyncio
from asyncio import Queue
from typing import List, Optional, Dict, Any
from dataclasses import dataclass
import logging
from datetime import datetime, timezone

from .pdf_processor import PdfProcessor
from .pdf_chunker import PdfChunker
from .pdf_embedder import PdfEmbedder
from .repository import PdfRepository
from .schema import PdfProcessingRequest, ChunkDocument
from schema import ProcessedContent
from infra.core.config import settings
from infra.core.processing_logger import processing_logger

logger = logging.getLogger(__name__)

@dataclass
class BatchTask:
    """배치 처리 태스크"""
    batch_id: int
    chunks: List[ChunkDocument]
    document_id: str

class PdfOrchestrator:
    """스트리밍 멀티플렉싱 PDF 처리 오케스트레이터"""
    
    def __init__(self, chunking_strategy: str = None):
        self.processor = PdfProcessor()
        self.chunker = PdfChunker(strategy=chunking_strategy)
        self.embedder = PdfEmbedder()
        self.repository = PdfRepository()
        
        # 배치 설정
        self.batch_size = settings.PDF_BATCH_SIZE
        self.max_concurrent_batches = settings.PDF_MAX_CONCURRENT_BATCHES
        
        # 큐 및 상태 관리
        self.chunk_queue: Queue[Optional[ChunkDocument]] = Queue()
        self.batch_queue: Queue[BatchTask] = Queue(maxsize=self.max_concurrent_batches * 2)
        self.processing_results: Queue[Dict[str, Any]] = Queue()
        
        # 전략 정보 로깅
        strategy_info = self.chunker.get_strategy_info()
        logger.info(f"PDF Orchestrator initialized with chunking strategy: {strategy_info}")
    
    async def process_pdf(self, request: PdfProcessingRequest) -> ProcessedContent:
        """스트리밍 멀티플렉싱 PDF 처리"""
        try:
            start_time = asyncio.get_event_loop().time()
            
            # 1. 텍스트 추출
            processing_logger.text_extraction_started(request.document_id)
            text_start = asyncio.get_event_loop().time()
            
            extracted_text, file_content = await self.processor.extract_text_to_memory(
                request.document_id
            )
            
            text_duration = asyncio.get_event_loop().time() - text_start
            processing_logger.text_extraction_completed(
                document_id=request.document_id,
                duration=text_duration,
                text_length=len(extracted_text)
            )
            
            # 메모리 해제
            del file_content
            
            # 2. 동시 실행할 태스크들
            tasks = []
            
            # 2-1. 청킹 + 큐 추가 태스크
            chunking_task = asyncio.create_task(
                self._chunk_and_enqueue(
                    document_id=request.document_id,
                    text=extracted_text,
                    metadata=request.metadata
                )
            )
            tasks.append(chunking_task)
            
            # 2-2. 배치 생성 태스크
            batch_creation_task = asyncio.create_task(
                self._create_batches_from_queue(request.document_id)
            )
            tasks.append(batch_creation_task)
            
            # 2-3. 배치 처리 워커들 (멀티플렉싱)
            for worker_id in range(self.max_concurrent_batches):
                worker_task = asyncio.create_task(
                    self._batch_processing_worker(worker_id)
                )
                tasks.append(worker_task)
            
            # 2-4. 결과 수집 태스크
            result_collector_task = asyncio.create_task(
                self._collect_results()
            )
            tasks.append(result_collector_task)
            
            # 3. 모든 태스크 실행 및 대기
            await asyncio.gather(*tasks[:-1])  # 결과 수집 태스크 제외
            
            # 4. 처리 완료 신호
            await self.processing_results.put(None)
            
            # 5. 최종 결과 수집
            final_results = await result_collector_task
            
            # 6. 처리 시간 계산
            processing_time = asyncio.get_event_loop().time() - start_time
            
            processing_logger.processing_completed(
                document_id=request.document_id,
                total_duration=processing_time,
                stats=final_results
            )
            
            # 7. 상태 업데이트
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="completed",
                processed_info=final_results
            )
            
            return ProcessedContent(
                content_id=request.document_id,
                original_type="pdf",
                processed_data=final_results,
                metadata=request.metadata,
                processed_at=datetime.now(timezone.utc)
            )
            
        except Exception as e:
            processing_logger.error(
                document_id=request.document_id,
                step="pdf_processing",
                error=str(e)
            )
            await self.repository.update_processing_status(
                document_id=request.document_id,
                status="failed",
                error_message=str(e)
            )
            raise
    
    async def _chunk_and_enqueue(
        self, 
        document_id: str, 
        text: str, 
        metadata: Dict[str, Any]
    ) -> int:
        """텍스트를 청킹하면서 동시에 큐에 추가"""
        try:
            chunk_count = 0
            
            processing_logger.chunking_started(document_id)
            chunk_start = asyncio.get_event_loop().time()
            
            # 스트리밍 방식으로 청킹
            async for chunk in self.chunker.create_chunks_streaming(
                document_id=document_id,
                text=text,
                metadata=metadata
            ):
                await self.chunk_queue.put(chunk)
                chunk_count += 1
                
                if chunk_count % 100 == 0:
                    logger.debug(f"Chunked {chunk_count} chunks so far...")
            
            # 청킹 완료 신호
            await self.chunk_queue.put(None)
            
            chunk_duration = asyncio.get_event_loop().time() - chunk_start
            processing_logger.chunking_completed(
                document_id=document_id,
                duration=chunk_duration,
                chunk_count=chunk_count
            )
            
            return chunk_count
            
        except Exception as e:
            logger.error(f"Chunking failed: {e}")
            await self.chunk_queue.put(None)  # 완료 신호
            raise
    
    async def _create_batches_from_queue(self, document_id: str) -> int:
        """큐에서 청크를 가져와 배치 생성"""
        batch_count = 0
        current_batch = []
        
        try:
            while True:
                chunk = await self.chunk_queue.get()
                
                if chunk is None:
                    # 남은 청크로 마지막 배치 생성
                    if current_batch:
                        batch_task = BatchTask(
                            batch_id=batch_count,
                            chunks=current_batch,
                            document_id=document_id
                        )
                        await self.batch_queue.put(batch_task)
                        batch_count += 1
                    
                    # 배치 생성 완료 신호
                    for _ in range(self.max_concurrent_batches):
                        await self.batch_queue.put(None)
                    
                    break
                
                current_batch.append(chunk)
                
                if len(current_batch) >= self.batch_size:
                    batch_task = BatchTask(
                        batch_id=batch_count,
                        chunks=current_batch,
                        document_id=document_id
                    )
                    await self.batch_queue.put(batch_task)
                    batch_count += 1
                    current_batch = []
                    
                    logger.debug(f"Created batch {batch_count} with {self.batch_size} chunks")
            
            logger.info(f"Batch creation completed: {batch_count} batches created")
            return batch_count
            
        except Exception as e:
            logger.error(f"Batch creation failed: {e}")
            for _ in range(self.max_concurrent_batches):
                await self.batch_queue.put(None)
            raise
    
    async def _batch_processing_worker(self, worker_id: int):
        """배치 처리 워커 (멀티플렉싱)"""
        logger.info(f"Worker {worker_id} started")
        
        try:
            while True:
                batch_task = await self.batch_queue.get()
                
                if batch_task is None:
                    break
                
                result = await self._process_single_batch(
                    batch_task=batch_task,
                    worker_id=worker_id
                )
                
                await self.processing_results.put(result)
                
        except Exception as e:
            logger.error(f"Worker {worker_id} failed: {e}")
            await self.processing_results.put({
                "error": str(e),
                "worker_id": worker_id
            })
        
        logger.info(f"Worker {worker_id} stopped")
    
    async def _process_single_batch(
        self, 
        batch_task: BatchTask,
        worker_id: int
    ) -> Dict[str, Any]:
        """단일 배치 처리 (트랜잭션 포함)"""
        start_time = asyncio.get_event_loop().time()
        
        try:
            logger.info(
                f"Worker {worker_id} processing batch {batch_task.batch_id} "
                f"with {len(batch_task.chunks)} chunks"
            )
            
            # 트랜잭션으로 처리
            async with self.repository.create_transaction() as transaction:
                # 1. MongoDB에 청크 저장
                chunk_ids = await self.repository.save_chunks_batch(
                    batch_task.chunks, 
                    transaction=transaction
                )
                
                # 2. 임베딩 생성 (트랜잭션 외부)
                embeddings, index_info_map = await self.embedder.generate_batch_embeddings(
                    batch_task.chunks
                )
                
                # 3. Qdrant에 임베딩 저장 (MongoDB 참조 포함)
                await self.repository.save_embeddings_with_references(
                    embeddings=embeddings,
                    chunks=batch_task.chunks
                )
                
                # 4. MongoDB에 임베딩 정보 업데이트
                for chunk_id, embedding_id in index_info_map.items():
                    await self.repository.update_chunk_embedding_info(
                        chunk_id=chunk_id,
                        embedding_id=embedding_id,
                        transaction=transaction
                    )
                
                # 트랜잭션 커밋
                await transaction.commit()
            
            processing_time = asyncio.get_event_loop().time() - start_time
            
            return {
                "batch_id": batch_task.batch_id,
                "worker_id": worker_id,
                "chunks_saved": len(chunk_ids),
                "embeddings_created": len(embeddings),
                "processing_time": processing_time,
                "success": True
            }
            
        except Exception as e:
            logger.error(
                f"Worker {worker_id} failed to process batch {batch_task.batch_id}: {e}"
            )
            
            return {
                "batch_id": batch_task.batch_id,
                "worker_id": worker_id,
                "chunks_saved": 0,
                "embeddings_created": 0,
                "error": str(e),
                "success": False
            }
    
    async def _collect_results(self) -> Dict[str, Any]:
        """처리 결과 수집"""
        results = []
        
        while True:
            result = await self.processing_results.get()
            
            if result is None:
                break
            
            results.append(result)
        
        # 결과 집계
        total_chunks = sum(r.get('chunks_saved', 0) for r in results)
        total_embeddings = sum(r.get('embeddings_created', 0) for r in results)
        failed_batches = [r for r in results if not r.get('success', True)]
        
        avg_processing_time = sum(r.get('processing_time', 0) for r in results) / len(results) if results else 0
        
        return {
            "total_chunks": total_chunks,
            "total_embeddings": total_embeddings,
            "total_batches": len(results),
            "failed_batches": len(failed_batches),
            "avg_batch_processing_time": avg_processing_time,
            "errors": [r.get('error') for r in failed_batches if r.get('error')]
        }
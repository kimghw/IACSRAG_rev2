# test_batch_timing.py
"""배치 처리 타이밍 상세 분석"""

import asyncio
import sys
import os
import time
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from content_pdf.services.batch_processor import BatchProcessor
from content_pdf.services.embedding_service import EmbeddingService
from content_pdf.services.storage_service import StorageService
from content_pdf.schema import ChunkDocument
from infra.core.config import settings

# 상세 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class MockStorageService:
    """테스트용 가짜 저장 서비스"""
    async def save_chunks(self, chunks):
        await asyncio.sleep(0.1)  # 저장 시뮬레이션
        return [c.chunk_id for c in chunks]
    
    async def save_embeddings(self, embeddings, chunks):
        await asyncio.sleep(0.1)  # 저장 시뮬레이션

class TimingBatchProcessor(BatchProcessor):
    """타이밍 측정용 배치 프로세서"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.batch_timings = []
    
    async def _process_batch(self):
        """배치 처리 with 상세 타이밍"""
        batch_num = len(self.batch_timings) + 1
        batch_size = len(self.current_batch)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"🔄 Processing Batch #{batch_num} ({batch_size} chunks)")
        logger.info(f"{'='*60}")
        
        batch_start = time.time()
        timing = {'batch_num': batch_num, 'size': batch_size}
        
        # 1. 청크 저장
        save_start = time.time()
        chunk_ids = await self.storage_service.save_chunks(self.current_batch)
        timing['save_chunks'] = time.time() - save_start
        logger.info(f"  ✓ Chunks saved: {timing['save_chunks']:.3f}s")
        
        # 2. 임베딩 생성
        embed_start = time.time()
        embeddings = await self.embedding_service.generate_embeddings_batch(
            self.current_batch
        )
        timing['embeddings'] = time.time() - embed_start
        logger.info(f"  ✓ Embeddings created: {timing['embeddings']:.3f}s")
        
        # 3. 벡터 저장
        vector_start = time.time()
        await self.storage_service.save_embeddings(embeddings, self.current_batch)
        timing['save_vectors'] = time.time() - vector_start
        logger.info(f"  ✓ Vectors saved: {timing['save_vectors']:.3f}s")
        
        timing['total'] = time.time() - batch_start
        logger.info(f"  📊 Batch total: {timing['total']:.3f}s")
        
        self.batch_timings.append(timing)
        self.current_batch = []
        self.stats['batches_processed'] += 1

async def test_batch_processing():
    """배치 처리 타이밍 테스트"""
    
    logger.info(f"\n🔧 Configuration:")
    logger.info(f"  - Batch size: {settings.PDF_BATCH_SIZE}")
    logger.info(f"  - Sub-batch size: {settings.PDF_SUB_BATCH_SIZE}")
    
    # 서비스 생성
    embedding_service = EmbeddingService()
    storage_service = MockStorageService()
    
    # 다양한 청크 수로 테스트
    test_cases = [30, 50, 62, 100]
    
    for total_chunks in test_cases:
        logger.info(f"\n\n{'#'*70}")
        logger.info(f"📋 Testing with {total_chunks} chunks")
        logger.info(f"{'#'*70}")
        
        # 배치 프로세서 생성
        processor = TimingBatchProcessor(
            batch_size=settings.PDF_BATCH_SIZE,
            embedding_service=embedding_service,
            storage_service=storage_service,
            document_id="test-doc"
        )
        
        # 청크 생성 및 추가 (스트리밍 시뮬레이션)
        overall_start = time.time()
        
        for i in range(total_chunks):
            chunk = ChunkDocument(
                document_id="test-doc",
                chunk_id=f"chunk-{i}",
                chunk_index=i,
                text_content=f"Test chunk {i} content " * 20,
                char_start=i*1000,
                char_end=(i+1)*1000,
                chunk_metadata={},
                created_at=datetime.now(timezone.utc)
            )
            
            # 청크 추가
            await processor.add_chunk(chunk)
            
            # 스트리밍 시뮬레이션 (약간의 딜레이)
            if i % 10 == 0:
                await asyncio.sleep(0.001)
        
        # 남은 청크 처리
        logger.info(f"\n🔄 Flushing remaining chunks...")
        await processor.flush()
        
        overall_time = time.time() - overall_start
        
        # 결과 분석
        logger.info(f"\n\n📊 Summary for {total_chunks} chunks:")
        logger.info(f"{'='*60}")
        logger.info(f"Total processing time: {overall_time:.3f}s")
        logger.info(f"Batches processed: {len(processor.batch_timings)}")
        
        total_embed_time = sum(t['embeddings'] for t in processor.batch_timings)
        logger.info(f"Total embedding time: {total_embed_time:.3f}s")
        logger.info(f"Average per chunk: {total_embed_time/total_chunks*1000:.1f}ms")
        
        # 배치별 상세 정보
        logger.info(f"\nBatch details:")
        for t in processor.batch_timings:
            logger.info(f"  Batch {t['batch_num']}: {t['size']} chunks → {t['embeddings']:.3f}s embedding")
        
        # 예상 배치 수 vs 실제
        expected_batches = (total_chunks + settings.PDF_BATCH_SIZE - 1) // settings.PDF_BATCH_SIZE
        logger.info(f"\nExpected batches: {expected_batches}")
        logger.info(f"Actual batches: {len(processor.batch_timings)}")

if __name__ == "__main__":
    asyncio.run(test_batch_processing())
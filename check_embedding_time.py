# check_embedding_time.py
"""임베딩 시간 확인"""

import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from content_pdf.services.embedding_service import EmbeddingService
from content_pdf.schema import ChunkDocument
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_embedding_time():
    """임베딩 시간 직접 측정"""
    
    # 테스트 청크 생성
    test_chunks = []
    for i in range(20):  # 20개 청크 (SUB_BATCH_SIZE와 동일)
        chunk = ChunkDocument(
            document_id="test-doc",
            chunk_id=f"chunk-{i}",
            chunk_index=i,
            text_content=f"This is test chunk number {i}. " * 50,  # 적당한 길이
            char_start=i*1000,
            char_end=(i+1)*1000,
            chunk_metadata={},
            created_at=datetime.now(timezone.utc)
        )
        test_chunks.append(chunk)
    
    # 임베딩 서비스
    embedding_service = EmbeddingService()
    
    logger.info(f"Testing embedding generation for {len(test_chunks)} chunks...")
    logger.info(f"Model: {embedding_service.model}")
    logger.info(f"Sub-batch size: {embedding_service.sub_batch_size}")
    
    # 시간 측정
    start_time = time.time()
    
    embeddings = await embedding_service.generate_embeddings_batch(test_chunks)
    
    end_time = time.time()
    duration = end_time - start_time
    
    logger.info(f"\n✅ Results:")
    logger.info(f"  - Chunks: {len(test_chunks)}")
    logger.info(f"  - Embeddings created: {len(embeddings)}")
    logger.info(f"  - Total time: {duration:.2f}s")
    logger.info(f"  - Time per chunk: {duration/len(test_chunks):.3f}s")
    
    # 더 큰 배치 테스트
    logger.info(f"\n📊 Testing larger batch (50 chunks)...")
    
    large_chunks = []
    for i in range(50):  # PDF_BATCH_SIZE와 동일
        chunk = ChunkDocument(
            document_id="test-doc-2",
            chunk_id=f"chunk-{i}",
            chunk_index=i,
            text_content=f"This is test chunk number {i}. " * 50,
            char_start=i*1000,
            char_end=(i+1)*1000,
            chunk_metadata={},
            created_at=datetime.now(timezone.utc)
        )
        large_chunks.append(chunk)
    
    start_time = time.time()
    embeddings = await embedding_service.generate_embeddings_batch(large_chunks)
    duration = time.time() - start_time
    
    logger.info(f"\n✅ Large batch results:")
    logger.info(f"  - Chunks: {len(large_chunks)}")
    logger.info(f"  - Embeddings created: {len(embeddings)}")
    logger.info(f"  - Total time: {duration:.2f}s")
    logger.info(f"  - Time per chunk: {duration/len(large_chunks):.3f}s")
    logger.info(f"  - Sub-batches: {len(large_chunks) // embedding_service.sub_batch_size}")

if __name__ == "__main__":
    asyncio.run(test_embedding_time())
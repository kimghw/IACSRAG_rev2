# test_chunking_speed.py
"""순수 청킹 속도만 테스트"""

import asyncio
import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from content_pdf.chunking.character_chunker import CharacterChunker
from content_pdf.chunking.semantic_chunker import SemanticChunker
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_pure_chunking_speed():
    """순수 청킹 속도만 측정 (임베딩 없이)"""
    
    # 다양한 크기의 텍스트 생성
    sizes = [1000, 10000, 50000, 100000]  # 1KB, 10KB, 50KB, 100KB
    
    for text_size in sizes:
        # 테스트 텍스트 생성
        test_text = "This is a test sentence. " * (text_size // 25)  # 약 25자
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing with {len(test_text):,} characters (~{text_size//1000}KB)")
        logger.info(f"{'='*60}")
        
        # Character Chunking 테스트
        char_chunker = CharacterChunker()
        chunks = []
        
        start_time = time.time()
        async for chunk in char_chunker.chunk_streaming(
            text=test_text,
            document_id="test",
            metadata={}
        ):
            chunks.append(chunk)
        char_time = time.time() - start_time
        
        logger.info(f"\nCharacter Chunking:")
        logger.info(f"  - Chunks created: {len(chunks)}")
        logger.info(f"  - Time taken: {char_time*1000:.1f}ms")
        logger.info(f"  - Speed: {len(test_text)/char_time/1000000:.1f} MB/s")
        
        # Semantic Chunking 테스트
        sem_chunker = SemanticChunker()
        chunks = []
        
        start_time = time.time()
        async for chunk in sem_chunker.chunk_streaming(
            text=test_text,
            document_id="test",
            metadata={}
        ):
            chunks.append(chunk)
        sem_time = time.time() - start_time
        
        logger.info(f"\nSemantic Chunking:")
        logger.info(f"  - Chunks created: {len(chunks)}")
        logger.info(f"  - Time taken: {sem_time*1000:.1f}ms")
        logger.info(f"  - Speed: {len(test_text)/sem_time/1000000:.1f} MB/s")
        
        logger.info(f"\nComparison:")
        logger.info(f"  - Semantic is {sem_time/char_time:.1f}x slower than Character")

async def test_real_pdf_content():
    """실제 PDF 텍스트와 유사한 내용으로 테스트"""
    
    # PDF에서 추출된 것과 유사한 텍스트
    pdf_like_text = """
[Page 1]
# Technical Document

## Introduction
This is a technical document that simulates real PDF content with various sections and formatting.

## Chapter 1: Overview
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.

### 1.1 Background
Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.

### 1.2 Objectives
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.

[Page 2]
## Chapter 2: Methodology
Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.

### 2.1 Approach
Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium.

### 2.2 Implementation
Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit.
""" * 10  # 10배로 늘려서 실제 PDF 크기와 유사하게
    
    logger.info(f"\n{'='*60}")
    logger.info("Testing with PDF-like content")
    logger.info(f"Text size: {len(pdf_like_text):,} characters")
    logger.info(f"{'='*60}")
    
    # Character chunking
    char_chunker = CharacterChunker()
    start = time.time()
    char_chunks = []
    async for chunk in char_chunker.chunk_streaming(pdf_like_text, "test", {}):
        char_chunks.append(chunk)
    char_time = time.time() - start
    
    logger.info(f"\nCharacter Chunking Results:")
    logger.info(f"  - Time: {char_time*1000:.1f}ms")
    logger.info(f"  - Chunks: {len(char_chunks)}")
    logger.info(f"  - Avg chunk size: {len(pdf_like_text)//len(char_chunks)} chars")

if __name__ == "__main__":
    asyncio.run(test_pure_chunking_speed())
    asyncio.run(test_real_pdf_content())
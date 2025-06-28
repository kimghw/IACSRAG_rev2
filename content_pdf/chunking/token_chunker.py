# content_pdf/chunking/token_chunker.py
import tiktoken
import uuid
import asyncio
from typing import AsyncGenerator, Dict, Any
from datetime import datetime, timezone

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings

class TokenChunker(ChunkingStrategy):
    """토큰 기반 청킹 전략"""
    
    def __init__(self):
        self.model = settings.PDF_TOKEN_MODEL
        self.max_tokens = settings.PDF_MAX_TOKENS_PER_CHUNK
        self.overlap_tokens = settings.PDF_TOKEN_OVERLAP
        self.encoding = tiktoken.encoding_for_model(self.model)
    
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """토큰 수 기반 스트리밍 청킹"""
        
        # 전체 텍스트 토큰화
        tokens = self.encoding.encode(text)
        total_tokens = len(tokens)
        
        chunk_index = 0
        position = 0
        
        while position < total_tokens:
            # 토큰 범위 계산
            chunk_end = min(position + self.max_tokens, total_tokens)
            chunk_tokens = tokens[position:chunk_end]
            
            # 토큰을 텍스트로 디코딩
            chunk_text = self.encoding.decode(chunk_tokens)
            
            # 원본 텍스트에서 위치 찾기
            char_start = text.find(chunk_text)
            char_end = char_start + len(chunk_text) if char_start != -1 else -1
            
            chunk = ChunkDocument(
                document_id=document_id,
                chunk_id=str(uuid.uuid4()),
                chunk_index=chunk_index,
                text_content=chunk_text,
                char_start=char_start if char_start != -1 else 0,
                char_end=char_end if char_end != -1 else len(chunk_text),
                token_count=len(chunk_tokens),
                chunk_metadata={
                    **metadata,
                    "chunking_strategy": "token",
                    "model": self.model,
                    "token_count": len(chunk_tokens),
                    "max_tokens_config": self.max_tokens,
                    "overlap_tokens_config": self.overlap_tokens
                },
                created_at=datetime.now(timezone.utc)
            )
            
            yield chunk
            chunk_index += 1
            
            # 다음 청크 시작 위치 (오버랩 고려)
            position = chunk_end - self.overlap_tokens
            
            # 무한 루프 방지
            if position >= chunk_end:
                break
            
            # CPU 양보
            if chunk_index % 5 == 0:
                await asyncio.sleep(0)
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수 (대략적인 계산)"""
        # 평균적으로 1 토큰 ≈ 4 문자
        estimated_tokens = text_length // 4
        if estimated_tokens == 0:
            return 0
            
        effective_tokens = self.max_tokens - self.overlap_tokens
        if effective_tokens <= 0:
            effective_tokens = self.max_tokens
            
        return (estimated_tokens // effective_tokens) + 1
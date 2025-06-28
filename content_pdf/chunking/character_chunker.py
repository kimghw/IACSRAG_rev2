# content_pdf/chunking/character_chunker.py
import uuid
import asyncio
from datetime import datetime, timezone
from typing import AsyncGenerator, Dict, Any

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings

class CharacterChunker(ChunkingStrategy):
    """문자 기반 청킹 전략"""
    
    def __init__(self):
        self.chunk_size = settings.PDF_CHUNK_SIZE
        self.chunk_overlap = settings.PDF_CHUNK_OVERLAP
    
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """문자 수 기반 스트리밍 청킹"""
        
        chunk_index = 0
        position = 0
        text_length = len(text)
        
        while position < text_length:
            # 청크 시작 위치
            char_start = position
            
            # 청크 끝 위치 계산
            chunk_end = min(position + self.chunk_size, text_length)
            
            # 단어 경계에서 자르기 (문장 중간에 자르지 않도록)
            if chunk_end < text_length:
                # 뒤로 가면서 단어 경계 찾기
                while chunk_end > char_start and text[chunk_end] not in ' \n\t.,!?;:':
                    chunk_end -= 1
                
                # 단어 경계를 찾지 못한 경우 원래 위치 사용
                if chunk_end == char_start:
                    chunk_end = min(position + self.chunk_size, text_length)
            
            # 청크 텍스트 추출
            chunk_text = text[char_start:chunk_end].strip()
            
            if chunk_text:  # 빈 청크 제외
                chunk = ChunkDocument(
                    document_id=document_id,
                    chunk_id=str(uuid.uuid4()),
                    chunk_index=chunk_index,
                    text_content=chunk_text,
                    char_start=char_start,
                    char_end=chunk_end,
                    chunk_metadata={
                        **metadata,
                        "chunking_strategy": "character",
                        "chunk_size": len(chunk_text),
                        "chunk_size_config": self.chunk_size,
                        "overlap_config": self.chunk_overlap
                    },
                    created_at=datetime.now(timezone.utc)
                )
                
                yield chunk
                chunk_index += 1
            
            # 다음 청크 시작 위치 (오버랩 고려)
            position = chunk_end - self.chunk_overlap
            
            # 무한 루프 방지
            if position <= char_start:
                position = chunk_end
            
            # CPU 양보
            if chunk_index % 10 == 0:
                await asyncio.sleep(0)
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수"""
        if text_length == 0:
            return 0
        
        effective_chunk_size = self.chunk_size - self.chunk_overlap
        if effective_chunk_size <= 0:
            effective_chunk_size = self.chunk_size
            
        return (text_length // effective_chunk_size) + 1
# content_pdf/chunking/recursive_semantic_chunker.py
import re
from typing import List, AsyncGenerator, Dict, Any, Tuple
from datetime import datetime, timezone
import uuid
import asyncio

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings
import logging

logger = logging.getLogger(__name__)

class RecursiveSemanticChunker(ChunkingStrategy):
    """재귀적 의미 단위 청킹 (LangChain 스타일)"""
    
    def __init__(self):
        self.chunk_size = settings.PDF_CHUNK_SIZE
        self.chunk_overlap = settings.PDF_CHUNK_OVERLAP
        
        # 분할 구분자 (우선순위 순)
        self.separators = [
            "\n\n\n",    # 여러 빈 줄
            "\n\n",      # 문단 구분
            "\n",        # 줄바꿈
            ". ",        # 문장
            "! ",        # 느낌표
            "? ",        # 물음표
            "; ",        # 세미콜론
            ", ",        # 쉼표
            " ",         # 공백
            ""           # 문자
        ]
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """재귀적 의미 단위 청킹"""
        
        # 텍스트를 재귀적으로 분할
        chunks = await self._recursive_split(text)
        
        # 오버랩 적용하여 최종 청크 생성
        final_chunks = self._apply_overlap(chunks)
        
        # 청크 문서 생성
        char_position = 0
        for chunk_index, chunk_text in enumerate(final_chunks):
            if not chunk_text.strip():
                continue
            
            # 원본 텍스트에서 위치 찾기
            char_start = text.find(chunk_text, char_position)
            if char_start == -1:
                char_start = char_position
            
            char_end = char_start + len(chunk_text)
            
            yield ChunkDocument(
                document_id=document_id,
                chunk_id=str(uuid.uuid4()),
                chunk_index=chunk_index,
                text_content=chunk_text,
                char_start=char_start,
                char_end=char_end,
                chunk_metadata={
                    **metadata,
                    "chunking_strategy": "recursive_semantic",
                    "chunk_size": len(chunk_text),
                    "chunk_size_config": self.chunk_size,
                    "overlap_config": self.chunk_overlap
                },
                created_at=datetime.now(timezone.utc)
            )
            
            char_position = char_start + len(chunk_text) - self.chunk_overlap
            
            # CPU 양보
            if chunk_index % 10 == 0:
                await asyncio.sleep(0)
    
    async def _recursive_split(self, text: str, separators: List[str] = None) -> List[str]:
        """재귀적으로 텍스트 분할"""
        if separators is None:
            separators = self.separators
        
        # 기본 케이스: 텍스트가 충분히 작으면 반환
        if len(text) <= self.chunk_size:
            return [text]
        
        # 현재 구분자로 분할 시도
        for i, separator in enumerate(separators):
            if separator == "":
                # 마지막 수단: 문자 단위로 분할
                return self._split_by_char(text)
            
            if separator in text:
                splits = text.split(separator)
                
                # 분할된 조각들을 청크 크기에 맞게 병합
                chunks = []
                current_chunk = ""
                
                for j, split in enumerate(splits):
                    # 구분자를 다시 추가 (마지막 조각 제외)
                    if j < len(splits) - 1:
                        split += separator
                    
                    # 현재 청크에 추가 가능한지 확인
                    if len(current_chunk) + len(split) <= self.chunk_size:
                        current_chunk += split
                    else:
                        if current_chunk:
                            chunks.append(current_chunk)
                        
                        # 분할이 여전히 너무 크면 재귀적으로 처리
                        if len(split) > self.chunk_size:
                            # 다음 구분자로 재귀 호출
                            sub_chunks = await self._recursive_split(
                                split, 
                                separators[i+1:] if i+1 < len(separators) else [""]
                            )
                            chunks.extend(sub_chunks)
                            current_chunk = ""
                        else:
                            current_chunk = split
                
                if current_chunk:
                    chunks.append(current_chunk)
                
                return chunks
        
        # 구분자를 찾지 못한 경우
        return self._split_by_char(text)
    
    def _split_by_char(self, text: str) -> List[str]:
        """문자 단위로 분할 (최후의 수단)"""
        chunks = []
        for i in range(0, len(text), self.chunk_size):
            chunk = text[i:i + self.chunk_size]
            
            # 단어 중간에 자르지 않도록 조정
            if i + self.chunk_size < len(text):
                # 뒤로 가면서 공백 찾기
                last_space = chunk.rfind(' ')
                if last_space > self.chunk_size // 2:  # 최소 절반 이상은 유지
                    chunk = chunk[:last_space]
            
            chunks.append(chunk)
        
        return chunks
    
    def _apply_overlap(self, chunks: List[str]) -> List[str]:
        """청크에 오버랩 적용"""
        if self.chunk_overlap <= 0 or len(chunks) <= 1:
            return chunks
        
        overlapped_chunks = []
        
        for i, chunk in enumerate(chunks):
            if i == 0:
                overlapped_chunks.append(chunk)
            else:
                # 이전 청크의 마지막 부분을 현재 청크 앞에 추가
                prev_chunk = chunks[i-1]
                overlap_text = self._get_overlap_text(prev_chunk)
                overlapped_chunk = overlap_text + chunk
                
                # 크기 제한 확인
                if len(overlapped_chunk) > self.chunk_size * 1.2:  # 20% 여유
                    overlapped_chunk = overlapped_chunk[:self.chunk_size]
                
                overlapped_chunks.append(overlapped_chunk)
        
        return overlapped_chunks
    
    def _get_overlap_text(self, text: str) -> str:
        """오버랩 텍스트 추출"""
        if len(text) <= self.chunk_overlap:
            return text
        
        overlap_text = text[-self.chunk_overlap:]
        
        # 단어 경계에서 시작하도록 조정
        first_space = overlap_text.find(' ')
        if first_space > 0 and first_space < self.chunk_overlap // 2:
            overlap_text = overlap_text[first_space+1:]
        
        return overlap_text
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수"""
        if text_length == 0:
            return 0
        
        effective_chunk_size = self.chunk_size - self.chunk_overlap
        if effective_chunk_size <= 0:
            effective_chunk_size = self.chunk_size
        
        return (text_length // effective_chunk_size) + 1
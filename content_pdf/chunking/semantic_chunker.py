# content_pdf/chunking/semantic_chunker.py
import re
import uuid
import asyncio
from typing import AsyncGenerator, Dict, Any, List
from datetime import datetime, timezone

from .base import ChunkingStrategy
from ..schema import ChunkDocument
from infra.core.config import settings

import logging
logger = logging.getLogger(__name__)

class SemanticChunker(ChunkingStrategy):
    """의미 기반 청킹 전략"""
    
    def __init__(self):
        self.max_chunk_size = settings.PDF_CHUNK_SIZE
        self.min_chunk_size = settings.PDF_CHUNK_SIZE // 4  # 최소 크기
        self.overlap_size = settings.PDF_CHUNK_OVERLAP
        
        # 의미적 경계 패턴들
        self.section_patterns = [
            r'^#{1,6}\s+',  # Markdown 헤더
            r'^\d+\.\s+',    # 번호 매기기 (1. 2. 3.)
            r'^[A-Z][A-Z\s]{2,}$',  # 대문자 제목
            r'^\[Page \d+\]',  # 페이지 구분자
            r'^Chapter \d+',   # 챕터
            r'^Section \d+',   # 섹션
        ]
        
        # 문장 종결 패턴
        self.sentence_endings = r'[.!?][\s\n]'
        
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """의미 단위로 텍스트 청킹"""
        
        # 전처리: 여러 개의 빈 줄을 하나로
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        
        # 의미적 섹션으로 분할
        sections = self._split_into_sections(text)
        
        chunk_index = 0
        char_position = 0
        
        for section_idx, section in enumerate(sections):
            # 각 섹션을 청크로 처리
            if not section.strip():
                continue
            
            # 섹션이 너무 크면 추가 분할
            if len(section) > self.max_chunk_size:
                # 문장 단위로 분할
                sentences = self._split_into_sentences(section)
                
                current_chunk_text = ""
                chunk_start = char_position
                
                for sentence in sentences:
                    # 현재 청크에 문장 추가 가능한지 확인
                    if len(current_chunk_text) + len(sentence) <= self.max_chunk_size:
                        current_chunk_text += sentence
                    else:
                        # 현재 청크 생성
                        if current_chunk_text.strip():
                            yield self._create_chunk(
                                text=current_chunk_text.strip(),
                                chunk_index=chunk_index,
                                document_id=document_id,
                                metadata=metadata,
                                char_start=chunk_start,
                                char_end=chunk_start + len(current_chunk_text)
                            )
                            chunk_index += 1
                        
                        # 새 청크 시작 (오버랩 적용)
                        overlap_text = self._get_overlap_text(current_chunk_text)
                        current_chunk_text = overlap_text + sentence
                        chunk_start = char_position - len(overlap_text)
                    
                    char_position += len(sentence)
                
                # 마지막 남은 텍스트 처리
                if current_chunk_text.strip():
                    yield self._create_chunk(
                        text=current_chunk_text.strip(),
                        chunk_index=chunk_index,
                        document_id=document_id,
                        metadata=metadata,
                        char_start=chunk_start,
                        char_end=chunk_start + len(current_chunk_text)
                    )
                    chunk_index += 1
            else:
                # 섹션이 적절한 크기면 그대로 청크로
                if section.strip() and len(section.strip()) >= self.min_chunk_size:
                    yield self._create_chunk(
                        text=section.strip(),
                        chunk_index=chunk_index,
                        document_id=document_id,
                        metadata=metadata,
                        char_start=char_position,
                        char_end=char_position + len(section)
                    )
                    chunk_index += 1
                
                char_position += len(section)
            
            # CPU 양보 (100개마다)
            if chunk_index % 100 == 0:
                await asyncio.sleep(0)
    
    def _split_into_sections(self, text: str) -> List[str]:
        """텍스트를 의미적 섹션으로 분할"""
        sections = []
        current_section = []
        
        lines = text.split('\n')
        
        for i, line in enumerate(lines):
            # 의미적 경계 확인
            is_boundary = any(re.match(pattern, line) for pattern in self.section_patterns)
            
            if is_boundary and current_section:
                # 현재 섹션 저장
                sections.append('\n'.join(current_section))
                current_section = [line]
            else:
                current_section.append(line)
            
            # 빈 줄이 연속으로 나오면 섹션 구분
            if i > 0 and not line.strip() and not lines[i-1].strip():
                if current_section[:-1]:  # 빈 줄 제외
                    sections.append('\n'.join(current_section[:-1]))
                    current_section = []
        
        # 마지막 섹션 처리
        if current_section:
            sections.append('\n'.join(current_section))
        
        return sections
    
    def _split_into_sentences(self, text: str) -> List[str]:
        """텍스트를 문장으로 분할"""
        # 간단한 문장 분할 (고급 처리는 NLTK나 spaCy 사용 가능)
        sentences = re.split(self.sentence_endings, text)
        
        # 문장 끝 문자 복원
        result = []
        for i, sentence in enumerate(sentences[:-1]):
            # 다음 문자가 문장 끝 문자인지 확인
            next_char_match = re.search(r'^[.!?]', text[len(''.join(sentences[:i+1])):])
            if next_char_match:
                sentence += next_char_match.group()
            result.append(sentence + ' ')
        
        if sentences:
            result.append(sentences[-1])
        
        return [s for s in result if s.strip()]
    
    def _get_overlap_text(self, text: str) -> str:
        """오버랩을 위한 텍스트 추출"""
        if len(text) <= self.overlap_size:
            return text
        
        # 마지막 overlap_size 만큼의 텍스트
        overlap_text = text[-self.overlap_size:]
        
        # 단어 경계에서 시작하도록 조정
        first_space = overlap_text.find(' ')
        if first_space > 0:
            overlap_text = overlap_text[first_space+1:]
        
        return overlap_text
    
    def _create_chunk(
        self,
        text: str,
        chunk_index: int,
        document_id: str,
        metadata: Dict[str, Any],
        char_start: int,
        char_end: int
    ) -> ChunkDocument:
        """청크 문서 생성"""
        return ChunkDocument(
            document_id=document_id,
            chunk_id=str(uuid.uuid4()),
            chunk_index=chunk_index,
            text_content=text,
            char_start=char_start,
            char_end=char_end,
            chunk_metadata={
                **metadata,
                "chunking_strategy": "semantic",
                "chunk_size": len(text),
                "chunk_size_config": self.max_chunk_size,
                "overlap_config": self.overlap_size
            },
            created_at=datetime.now(timezone.utc)
        )
    
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수 (대략적)"""
        if text_length == 0:
            return 0
        
        # 평균 청크 크기를 최대 크기의 70%로 가정
        avg_chunk_size = self.max_chunk_size * 0.7
        return int(text_length / avg_chunk_size) + 1
# content_pdf/chunking/base.py
from abc import ABC, abstractmethod
from typing import AsyncGenerator, Dict, Any

from ..schema import ChunkDocument

class ChunkingStrategy(ABC):
    """청킹 전략 베이스 클래스"""
    
    @abstractmethod
    async def chunk_streaming(
        self,
        text: str,
        document_id: str,
        metadata: Dict[str, Any]
    ) -> AsyncGenerator[ChunkDocument, None]:
        """스트리밍 방식으로 청킹"""
        pass
    
    @abstractmethod
    def estimate_chunks(self, text_length: int) -> int:
        """예상 청크 수 계산"""
        pass
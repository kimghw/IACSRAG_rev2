from typing import List, Dict, Any
import uuid
from langchain.text_splitter import RecursiveCharacterTextSplitter

from schema import ChunkData

class PdfChunker:
    """PDF 청킹 서비스"""
    
    def __init__(self):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=1000,
            chunk_overlap=200,
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", " ", ""]
        )
    
    async def create_chunks(
        self, 
        document_id: str, 
        text: str, 
        metadata: Dict[str, Any]
    ) -> List[ChunkData]:
        """텍스트를 의미론적 청크로 분할"""
        
        chunks_text = self.splitter.split_text(text)
        
        chunks = []
        for idx, chunk_text in enumerate(chunks_text):
            chunk = ChunkData(
                chunk_id=str(uuid.uuid4()),
                content_id=document_id,
                chunk_text=chunk_text,
                chunk_index=idx,
                metadata={
                    **metadata,
                    "chunk_size": len(chunk_text),
                    "total_chunks": len(chunks_text)
                }
            )
            chunks.append(chunk)
        
        return chunks
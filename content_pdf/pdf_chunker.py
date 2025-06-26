# content_pdf/pdf_chunker.py
from typing import List, Dict, Any
import uuid
from langchain.text_splitter import RecursiveCharacterTextSplitter
from datetime import datetime, timezone

from schema import ChunkData, ChunkDocument
from infra.core.config import settings

class PdfChunker:
    """PDF 청킹 서비스"""
    
    def __init__(self):
        self.splitter = RecursiveCharacterTextSplitter(
            chunk_size=settings.PDF_CHUNK_SIZE,
            chunk_overlap=settings.PDF_CHUNK_OVERLAP,
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", " ", ""]
        )
    
    async def create_chunks(
        self, 
        document_id: str, 
        text: str, 
        metadata: Dict[str, Any]
    ) -> List[ChunkDocument]:
        """텍스트를 의미론적 청크로 분할하여 ChunkDocument 리스트 반환"""
        
        chunks_text = self.splitter.split_text(text)
        
        chunk_documents = []
        for idx, chunk_text in enumerate(chunks_text):
            chunk_id = str(uuid.uuid4())
            
            # ChunkDocument 생성
            chunk_doc = ChunkDocument(
                document_id=document_id,
                chunk_id=chunk_id,
                chunk_data={
                    "text": chunk_text,
                    "metadata": {
                        **metadata,
                        "chunk_size": len(chunk_text),
                        "total_chunks": len(chunks_text),
                        "chunk_index": idx
                    }
                },
                chunk_index=idx,
                created_at=datetime.now(timezone.utc)
            )
            chunk_documents.append(chunk_doc)
        
        return chunk_documents
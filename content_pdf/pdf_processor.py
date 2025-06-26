# content_pdf/pdf_processor.py
import io
import pdfplumber
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from infra.databases.mongo_db import MongoDB
import logging

logger = logging.getLogger(__name__)

class PdfProcessor:
    """PDF 텍스트 추출 서비스"""
    
    def __init__(self):
        self.mongo = MongoDB()
    
    async def extract_text_to_memory(self, document_id: str) -> tuple[str, bytes]:
        """
        MongoDB GridFS에서 파일을 읽어 텍스트 추출
        Returns: (extracted_text, file_content_bytes)
        """
        # 1. 문서 메타데이터 조회
        doc = await self.mongo.find_one("uploads", {"document_id": document_id})
        if not doc:
            raise ValueError(f"Document not found: {document_id}")
        
        # 2. GridFS에서 파일 다운로드
        fs = AsyncIOMotorGridFSBucket(self.mongo.db)
        grid_out = await fs.open_download_stream(ObjectId(doc["gridfs_file_id"]))
        file_content = await grid_out.read()
        
        # 3. 메모리에서 직접 PDF 처리
        with pdfplumber.open(io.BytesIO(file_content)) as pdf:
            text = ""
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()
                if page_text:
                    text += f"[Page {page_num + 1}]\n{page_text}\n\n"
        
        logger.info(f"Extracted {len(text)} characters from PDF: {document_id}")
        
        return text.strip(), file_content
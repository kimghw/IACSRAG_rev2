import PyPDF2
import pdfplumber
from typing import Optional

# content_pdf/pdf_processor.py
class PdfProcessor:
    def __init__(self):
        self.mongo = MongoDB()
    
    async def extract_text(self, document_id: str) -> str:
        """MongoDB에서 파일 읽어서 텍스트 추출"""
        # GridFS에서 파일 가져오기
        doc = await self.mongo.find_one("uploads", {"document_id": document_id})
        
        fs = AsyncIOMotorGridFSBucket(self.mongo.db)
        file_content = await fs.download_to_stream(doc["gridfs_file_id"])
        
        # 메모리에서 직접 PDF 처리
        import io
        with pdfplumber.open(io.BytesIO(file_content)) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n"
        
        return text.strip()
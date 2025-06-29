# content_pdf/services/text_extractor.py
import io
import pdfplumber
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorGridFSBucket
from infra.databases.mongo_db import MongoDB
from typing import Tuple
import logging

logger = logging.getLogger(__name__)

class TextExtractor:
    """PDF/Markdown 텍스트 추출 서비스"""
    
    def __init__(self):
        self.mongo = MongoDB()
    
    async def extract(self, document_id: str) -> Tuple[str, str]:
        """
        MongoDB GridFS에서 파일을 읽어 텍스트 추출
        
        Returns:
            Tuple[str, str]: (추출된 텍스트, 콘텐츠 타입)
        """
        # 1. 문서 메타데이터 조회
        doc = await self.mongo.find_one("uploads", {"document_id": document_id})
        if not doc:
            raise ValueError(f"Document not found: {document_id}")
        
        # 2. GridFS에서 파일 다운로드
        fs = AsyncIOMotorGridFSBucket(self.mongo.db)
        grid_out = await fs.open_download_stream(ObjectId(doc["gridfs_file_id"]))
        file_content = await grid_out.read()
        
        # 3. 파일 타입 확인
        content_type = doc.get("content_type", "")
        filename = doc.get("filename", "")
        
        # 4. 파일 타입에 따라 처리 분기
        if content_type == "application/pdf" or filename.lower().endswith('.pdf'):
            text = await self._extract_pdf(file_content, document_id)
            return text, "pdf"
        elif content_type == "text/markdown" or filename.lower().endswith(('.md', '.markdown')):
            text = await self._extract_markdown(file_content, document_id)
            return text, "markdown"
        else:
            raise ValueError(f"Unsupported content type: {content_type} for document: {document_id}")
    
    async def _extract_pdf(self, file_content: bytes, document_id: str) -> str:
        """PDF에서 텍스트 추출"""
        try:
            with pdfplumber.open(io.BytesIO(file_content)) as pdf:
                text = ""
                for page_num, page in enumerate(pdf.pages):
                    page_text = page.extract_text()
                    if page_text:
                        text += f"[Page {page_num + 1}]\n{page_text}\n\n"
        except Exception as e:
            logger.error(f"Failed to extract text from PDF: {document_id}, Error: {e}")
            raise
        
        logger.info(f"Extracted {len(text)} characters from PDF: {document_id}")
        return text.strip()
    
    async def _extract_markdown(self, file_content: bytes, document_id: str) -> str:
        """Markdown에서 텍스트 추출 (그대로 디코딩)"""
        try:
            # Markdown은 이미 텍스트이므로 단순히 디코딩
            text = file_content.decode('utf-8')
        except UnicodeDecodeError:
            # UTF-8 실패시 다른 인코딩 시도
            try:
                text = file_content.decode('utf-8-sig')  # BOM 있는 UTF-8
            except:
                try:
                    text = file_content.decode('latin-1')  # 가장 관대한 인코딩
                except Exception as e:
                    logger.error(f"Failed to decode Markdown file: {document_id}, Error: {e}")
                    raise
        
        logger.info(f"Extracted {len(text)} characters from Markdown: {document_id}")
        return text.strip()
    
    # 하위 호환성을 위한 메서드 (기존 코드가 이 메서드를 호출하는 경우)
    async def extract_pdf(self, document_id: str) -> str:
        """PDF 텍스트 추출 - extract()로 위임"""
        text, _ = await self.extract(document_id)
        return text
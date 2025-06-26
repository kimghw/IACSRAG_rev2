import PyPDF2
import pdfplumber
from typing import Optional

class PdfProcessor:
    """PDF 텍스트 추출 서비스"""
    
    async def extract_text(self, file_path: str) -> str:
        """PDF에서 텍스트 추출"""
        text = ""
        
        # pdfplumber로 먼저 시도 (표 처리 우수)
        try:
            with pdfplumber.open(file_path) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"
        except:
            # 실패 시 PyPDF2로 재시도
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                for page_num in range(len(pdf_reader.pages)):
                    page = pdf_reader.pages[page_num]
                    text += page.extract_text() + "\n"
        
        return text.strip()
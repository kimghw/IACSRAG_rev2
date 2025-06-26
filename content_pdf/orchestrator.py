# content_pdf/orchestrator.py - extract_text 호출 부분 수정
async def process_pdf(self, request: PdfProcessingRequest) -> ProcessedContent:
    """
    PDF 처리 메인 플로우
    1. 텍스트 추출
    2. 청킹
    3. 임베딩 생성
    4. Qdrant 저장
    """
    try:
        logger.info(f"Starting PDF processing for document: {request.document_id}")
        
        # 1. 텍스트 추출 (document_id만 전달)
        extracted_text = await self.processor.extract_text(request.document_id)
        logger.info(f"Extracted {len(extracted_text)} characters from PDF")
        
        # 나머지 코드는 동일...
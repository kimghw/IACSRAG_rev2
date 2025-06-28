# test_pdf_complete.py
"""
PDF 처리 시스템 완전 테스트
- PDF 생성 및 업로드
- 처리 상태 모니터링
- 결과 검증
"""

import asyncio
import logging
import sys
import os
import time
from datetime import datetime
from pathlib import Path
import httpx
from typing import Dict, Any, List, Optional

# 프로젝트 루트 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.core.config import settings
from infra.databases.mongo_db import MongoDB
from infra.databases.qdrant_db import QdrantDB

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PDFTester:
    """PDF 처리 테스터"""
    
    def __init__(self):
        self.api_url = f"http://localhost:{settings.API_PORT}"
        self.mongo = MongoDB()
        self.qdrant = QdrantDB()
        
    def create_pdf(self, filename: str, size: str = "small") -> Optional[Path]:
        """테스트 PDF 생성"""
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            from reportlab.lib.units import inch
        except ImportError:
            logger.error("❌ reportlab not installed. Install with: pip install reportlab")
            return None
        
        # 테스트 디렉토리 생성
        test_dir = Path("test_pdfs")
        test_dir.mkdir(exist_ok=True)
        pdf_path = test_dir / filename
        
        # PDF 생성
        c = canvas.Canvas(str(pdf_path), pagesize=letter)
        width, height = letter
        
        # 제목
        c.setFont("Helvetica-Bold", 20)
        c.drawString(inch, height - inch, f"Test PDF - {size.upper()}")
        
        # 생성 시간
        c.setFont("Helvetica", 12)
        c.drawString(inch, height - 1.5*inch, f"Generated: {datetime.now()}")
        
        # 본문 내용
        y = height - 2.5*inch
        line_height = 15
        
        # 크기별 내용 생성
        content = self._generate_content(size)
        
        c.setFont("Helvetica", 11)
        for line in content:
            if y < inch:  # 새 페이지
                c.showPage()
                c.setFont("Helvetica", 11)
                y = height - inch
            
            # 제목은 굵게
            if line.startswith("##"):
                c.setFont("Helvetica-Bold", 14)
                c.drawString(inch, y, line.replace("##", "").strip())
                c.setFont("Helvetica", 11)
                y -= line_height * 1.5
            elif line.startswith("###"):
                c.setFont("Helvetica-Bold", 12)
                c.drawString(inch + 20, y, line.replace("###", "").strip())
                c.setFont("Helvetica", 11)
                y -= line_height * 1.3
            else:
                # 긴 줄 처리
                if len(line) > 70:
                    words = line.split()
                    current_line = ""
                    for word in words:
                        test_line = current_line + " " + word if current_line else word
                        if len(test_line) > 70:
                            c.drawString(inch, y, current_line)
                            y -= line_height
                            current_line = word
                        else:
                            current_line = test_line
                    if current_line:
                        c.drawString(inch, y, current_line)
                        y -= line_height
                else:
                    c.drawString(inch, y, line)
                    y -= line_height
        
        c.save()
        logger.info(f"✅ Created PDF: {pdf_path} ({pdf_path.stat().st_size:,} bytes)")
        return pdf_path
    
    def _generate_content(self, size: str) -> List[str]:
        """크기별 콘텐츠 생성"""
        base_content = [
            "",
            "## Introduction",
            "",
            "This is a test PDF document for the IACSRAG system.",
            "It tests the PDF processing pipeline including:",
            "- Text extraction with pdfplumber",
            "- Semantic chunking",
            "- Embedding generation",
            "- Vector storage in Qdrant",
            "",
            "## Section 1: System Overview",
            "",
            "The IACSRAG system processes various document types through an event-driven architecture.",
            "PDF documents are processed through multiple stages:",
            "",
            "1. Upload and validation",
            "2. GridFS storage in MongoDB",
            "3. Event publishing to Kafka",
            "4. Text extraction from PDF",
            "5. Semantic chunking of content",
            "6. Embedding generation via OpenAI",
            "7. Vector storage in Qdrant",
            "",
            "## Section 2: Technical Details",
            "",
            "### 2.1 Text Extraction",
            "",
            "The system uses pdfplumber for reliable text extraction from PDF files.",
            "It handles various PDF formats and encodings.",
            "",
            "### 2.2 Chunking Strategy",
            "",
            "The semantic chunking strategy identifies natural boundaries in the text:",
            "- Section headers",
            "- Paragraph breaks",
            "- Topic transitions",
            "",
            "This ensures each chunk maintains contextual coherence.",
            "",
            "## Section 3: Performance",
            "",
            "The system is optimized for performance:",
            "- Concurrent PDF processing",
            "- Batch embedding generation",
            "- Multiplexed API calls",
            "- Streaming chunk processing",
            "",
        ]
        
        if size == "small":
            content = base_content + [
                "## Conclusion",
                "",
                "This small test document verifies basic functionality.",
                ""
            ]
        
        elif size == "medium":
            content = base_content.copy()
            # 추가 섹션
            for i in range(4, 10):
                content.extend([
                    f"## Section {i}: Additional Content",
                    "",
                    f"This is section {i} with more detailed information.",
                    "Lorem ipsum dolor sit amet, consectetur adipiscing elit.",
                    "Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    "",
                    f"### {i}.1 Subsection",
                    "",
                    "Detailed technical information goes here.",
                    "The system processes this content efficiently.",
                    "",
                ])
        
        else:  # large
            content = base_content.copy()
            # 많은 섹션 추가
            for i in range(4, 20):
                content.extend([
                    f"## Section {i}: Extended Documentation",
                    "",
                    f"This is section {i} of the comprehensive test document.",
                    "It contains multiple paragraphs to test the chunking system thoroughly.",
                    "",
                    f"### {i}.1 Overview",
                    "",
                    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.",
                    "Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.",
                    "",
                    f"### {i}.2 Implementation Details",
                    "",
                    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur.",
                    "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.",
                    "",
                    f"### {i}.3 Best Practices",
                    "",
                    "Additional content to ensure comprehensive testing of the chunking system.",
                    "Each section should be properly identified and processed.",
                    "",
                ])
        
        return content
    
    async def upload_pdf(self, pdf_path: Path) -> Optional[Dict[str, Any]]:
        """PDF 업로드"""
        logger.info(f"\n{'='*50}")
        logger.info(f"📤 Uploading: {pdf_path.name}")
        
        start_time = time.time()
        
        with open(pdf_path, 'rb') as f:
            pdf_content = f.read()
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            files = {'file': (pdf_path.name, pdf_content, 'application/pdf')}
            
            response = await client.post(
                f"{self.api_url}/api/v1/upload/pdf",
                files=files
            )
            
            if response.status_code == 200:
                result = response.json()
                upload_time = time.time() - start_time
                
                logger.info(f"✅ Upload successful!")
                logger.info(f"   Document ID: {result['document_id']}")
                logger.info(f"   Upload time: {upload_time:.2f}s")
                
                return result
            else:
                logger.error(f"❌ Upload failed: {response.status_code}")
                logger.error(f"   {response.text}")
                return None
    
    async def monitor_processing(self, document_id: str, timeout: int = 60) -> Dict[str, Any]:
        """처리 상태 모니터링"""
        logger.info(f"\n⏳ Monitoring processing...")
        
        start_time = time.time()
        last_status = None
        result = {}
        
        while time.time() - start_time < timeout:
            doc = await self.mongo.find_one("uploads", {"document_id": document_id})
            
            if doc:
                status = doc.get('status', 'unknown')
                
                if status != last_status:
                    logger.info(f"   Status: {last_status} → {status}")
                    last_status = status
                
                if status == 'completed':
                    processing_time = time.time() - start_time
                    logger.info(f"\n✅ Processing completed in {processing_time:.2f}s")
                    
                    # 처리 정보 출력
                    if 'processing_info' in doc:
                        info = doc['processing_info']
                        logger.info(f"📊 Processing Statistics:")
                        logger.info(f"   - Total chunks: {info.get('total_chunks', 0)}")
                        logger.info(f"   - Chunks saved: {info.get('chunks_saved', 0)}")
                        logger.info(f"   - Embeddings created: {info.get('embeddings_created', 0)}")
                        logger.info(f"   - Batches processed: {info.get('batches_processed', 0)}")
                        
                        result = {
                            'status': 'completed',
                            'processing_time': processing_time,
                            'stats': info
                        }
                    break
                
                elif status == 'failed':
                    logger.error(f"\n❌ Processing failed: {doc.get('error_message')}")
                    result = {
                        'status': 'failed',
                        'error': doc.get('error_message')
                    }
                    break
            
            await asyncio.sleep(2)
        
        if not result:
            logger.warning(f"⏱️  Timeout after {timeout}s")
            result = {'status': 'timeout'}
        
        return result
    
    async def verify_results(self, document_id: str):
        """결과 검증"""
        logger.info(f"\n🔍 Verifying results...")
        
        # MongoDB 청크 확인
        chunks = []
        cursor = self.mongo.db.pdf_chunks.find(
            {"document_id": document_id}
        ).sort("chunk_index", 1)
        
        async for chunk in cursor:
            chunks.append(chunk)
        
        logger.info(f"\n📚 MongoDB Chunks: {len(chunks)}")
        
        if chunks:
            # 첫 번째 청크
            logger.info(f"\n   First chunk:")
            logger.info(f"   - Index: {chunks[0]['chunk_index']}")
            logger.info(f"   - Size: {len(chunks[0]['text_content'])} chars")
            logger.info(f"   - Preview: {chunks[0]['text_content'][:80]}...")
            
            # 마지막 청크
            if len(chunks) > 1:
                logger.info(f"\n   Last chunk:")
                logger.info(f"   - Index: {chunks[-1]['chunk_index']}")
                logger.info(f"   - Size: {len(chunks[-1]['text_content'])} chars")
                logger.info(f"   - Preview: {chunks[-1]['text_content'][:80]}...")
            
            # 청킹 전략
            strategy = chunks[0].get('chunk_metadata', {}).get('chunking_strategy', 'unknown')
            logger.info(f"\n   Chunking strategy: {strategy}")
            
            # 전체 텍스트 크기
            total_chars = sum(len(chunk['text_content']) for chunk in chunks)
            avg_size = total_chars // len(chunks) if chunks else 0
            logger.info(f"   Total text: {total_chars:,} chars")
            logger.info(f"   Average chunk size: {avg_size:,} chars")
        
        return chunks
    
    async def cleanup(self):
        """테스트 파일 정리"""
        test_dir = Path("test_pdfs")
        if test_dir.exists():
            import shutil
            shutil.rmtree(test_dir)
            logger.info("\n🧹 Cleaned up test files")


async def run_test(test_type: str = "small"):
    """테스트 실행"""
    tester = PDFTester()
    
    try:
        # 1. PDF 생성
        if test_type == "existing":
            # 기존 파일 사용
            if len(sys.argv) < 3:
                logger.error("Please provide PDF file path")
                return
            pdf_path = Path(sys.argv[2])
            if not pdf_path.exists():
                logger.error(f"File not found: {pdf_path}")
                return
        else:
            # 새 PDF 생성
            pdf_path = tester.create_pdf(f"test_{test_type}.pdf", test_type)
            if not pdf_path:
                return
        
        # 2. 업로드
        upload_result = await tester.upload_pdf(pdf_path)
        if not upload_result:
            return
        
        document_id = upload_result['document_id']
        
        # 3. 처리 모니터링
        processing_result = await tester.monitor_processing(document_id)
        
        # 4. 결과 검증
        if processing_result['status'] == 'completed':
            await tester.verify_results(document_id)
        
        # 5. 요약
        logger.info(f"\n{'='*50}")
        logger.info("📊 Test Summary")
        logger.info(f"{'='*50}")
        logger.info(f"Test type: {test_type}")
        logger.info(f"Document ID: {document_id}")
        logger.info(f"Status: {processing_result['status']}")
        
        if processing_result['status'] == 'completed':
            stats = processing_result.get('stats', {})
            logger.info(f"Processing time: {processing_result['processing_time']:.2f}s")
            logger.info(f"Chunks: {stats.get('total_chunks', 0)}")
            logger.info(f"Embeddings: {stats.get('embeddings_created', 0)}")
        
    finally:
        # 정리
        if test_type != "existing":
            await tester.cleanup()


async def run_concurrent_test(count: int = 3):
    """동시 처리 테스트"""
    logger.info(f"\n{'='*50}")
    logger.info(f"🚀 Concurrent Processing Test ({count} PDFs)")
    logger.info(f"{'='*50}")
    
    tester = PDFTester()
    
    try:
        # 1. 여러 PDF 생성
        pdf_files = []
        sizes = ["small", "medium", "large"]
        
        for i in range(count):
            size = sizes[i % len(sizes)]
            pdf_path = tester.create_pdf(f"concurrent_{i+1}_{size}.pdf", size)
            if pdf_path:
                pdf_files.append((pdf_path, size))
        
        # 2. 동시 업로드
        upload_tasks = []
        for pdf_path, size in pdf_files:
            task = tester.upload_pdf(pdf_path)
            upload_tasks.append((task, size))
        
        results = []
        for task, size in upload_tasks:
            result = await task
            if result:
                results.append((result['document_id'], size))
        
        logger.info(f"\n📊 Uploaded {len(results)}/{count} PDFs")
        
        # 3. 각 문서 처리 모니터링
        for doc_id, size in results:
            logger.info(f"\n--- Processing {size} PDF ({doc_id[:8]}...)")
            await tester.monitor_processing(doc_id, timeout=30)
        
    finally:
        await tester.cleanup()


async def main():
    """메인 함수"""
    # API 서버 확인
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{settings.API_PORT}/health")
            if response.status_code != 200:
                logger.error("❌ API server not running. Start with: python main.py")
                return
            logger.info("✅ API server is running")
    except:
        logger.error("❌ Cannot connect to API server")
        return
    
    # 테스트 타입 결정
    if len(sys.argv) > 1:
        test_type = sys.argv[1]
        
        if test_type == "concurrent":
            count = int(sys.argv[2]) if len(sys.argv) > 2 else 3
            await run_concurrent_test(count)
        elif test_type in ["small", "medium", "large", "existing"]:
            await run_test(test_type)
        else:
            logger.info("Usage:")
            logger.info("  python test_pdf_complete.py [small|medium|large|existing|concurrent]")
            logger.info("  python test_pdf_complete.py existing /path/to/file.pdf")
            logger.info("  python test_pdf_complete.py concurrent [count]")
    else:
        # 기본: small 테스트
        await run_test("small")


if __name__ == "__main__":
    asyncio.run(main())
# test_timing_detail.py
"""각 처리 단계별 상세 시간 측정"""

import asyncio
import logging
import sys
import os
import time
from datetime import datetime
from pathlib import Path
import httpx
from typing import Dict, Any, List
import re

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.core.config import settings
from infra.databases.mongo_db import MongoDB

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DetailedTimingTester:
    """상세 타이밍 테스터"""
    
    def __init__(self):
        self.api_url = f"http://localhost:{settings.API_PORT}"
        self.mongo = MongoDB()
        
    def create_test_pdf(self, name: str, chunks: int) -> Path:
        """특정 청크 수를 생성하는 PDF"""
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            from reportlab.lib.units import inch
        except ImportError:
            logger.error("reportlab not installed")
            return None
        
        test_dir = Path("test_timing")
        test_dir.mkdir(exist_ok=True)
        pdf_path = test_dir / f"{name}.pdf"
        
        c = canvas.Canvas(str(pdf_path), pagesize=letter)
        width, height = letter
        
        # 제목
        c.setFont("Helvetica-Bold", 20)
        c.drawString(inch, height - inch, f"Timing Test - {chunks} chunks expected")
        
        # 청크 크기를 고려한 내용 생성 (chunk_size=1000, overlap=200)
        c.setFont("Helvetica", 11)
        y = height - 2*inch
        
        # 각 청크는 약 800자 정도의 유효 텍스트 필요 (오버랩 고려)
        text_per_chunk = 800
        total_text_needed = text_per_chunk * chunks
        
        # 텍스트 생성
        text_lines = []
        for i in range(chunks):
            text_lines.append(f"## Section {i+1}")
            text_lines.append("")
            # 각 섹션에 충분한 텍스트 추가
            paragraph = f"This is section {i+1} of the timing test document. " * 20
            text_lines.append(paragraph)
            text_lines.append("")
        
        # PDF에 쓰기
        for line in text_lines:
            if y < inch:
                c.showPage()
                c.setFont("Helvetica", 11)
                y = height - inch
            
            if line.startswith("##"):
                c.setFont("Helvetica-Bold", 14)
                c.drawString(inch, y, line)
                c.setFont("Helvetica", 11)
                y -= 20
            else:
                # 긴 텍스트 줄바꿈
                words = line.split()
                current_line = ""
                for word in words:
                    if len(current_line + word) > 70:
                        c.drawString(inch, y, current_line)
                        y -= 15
                        current_line = word + " "
                    else:
                        current_line += word + " "
                if current_line:
                    c.drawString(inch, y, current_line)
                    y -= 15
        
        c.save()
        return pdf_path
    
    async def analyze_processing_logs(self, document_id: str) -> Dict[str, Any]:
        """처리 로그 분석"""
        # Processing 로그 파일 읽기 (설정에 따라)
        if settings.PROCESSING_LOG_MODE in ["file", "both"]:
            log_file = Path(settings.PROCESSING_LOG_FILE)
            if log_file.exists():
                with open(log_file, 'r') as f:
                    logs = f.readlines()
                
                # 해당 document_id에 대한 로그 필터링
                doc_logs = [l for l in logs if document_id in l]
                
                # 타이밍 정보 추출
                timings = {}
                for log in doc_logs:
                    if "텍스트 추출 완료" in log:
                        match = re.search(r'duration=(\d+\.\d+)s', log)
                        if match:
                            timings['text_extraction'] = float(match.group(1))
                    
                    elif "청킹 완료" in log:
                        match = re.search(r'duration=(\d+\.\d+)s', log)
                        if match:
                            timings['chunking'] = float(match.group(1))
                    
                    elif "임베딩 생성 완료" in log:
                        match = re.search(r'duration=(\d+\.\d+)s', log)
                        if match:
                            timings['embedding'] = float(match.group(1))
                
                return timings
        
        return {}
    
    async def test_single_pdf(self, chunks: int) -> Dict[str, Any]:
        """단일 PDF 테스트"""
        logger.info(f"\n{'='*60}")
        logger.info(f"📋 Testing PDF with ~{chunks} chunks")
        logger.info(f"{'='*60}")
        
        # 1. PDF 생성
        pdf_path = self.create_test_pdf(f"test_{chunks}_chunks", chunks)
        if not pdf_path:
            return None
        
        file_size = pdf_path.stat().st_size
        logger.info(f"Created PDF: {pdf_path.name} ({file_size:,} bytes)")
        
        # 2. 업로드
        upload_start = time.time()
        
        with open(pdf_path, 'rb') as f:
            pdf_content = f.read()
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            files = {'file': (pdf_path.name, pdf_content, 'application/pdf')}
            
            response = await client.post(
                f"{self.api_url}/api/v1/upload/pdf",
                files=files
            )
            
            upload_time = time.time() - upload_start
            
            if response.status_code != 200:
                logger.error(f"Upload failed: {response.status_code}")
                return None
            
            result = response.json()
            document_id = result['document_id']
            
        logger.info(f"✅ Upload complete in {upload_time:.2f}s")
        
        # 3. 처리 모니터링
        process_start = time.time()
        last_status = None
        
        while True:
            doc = await self.mongo.find_one("uploads", {"document_id": document_id})
            
            if doc:
                status = doc.get('status', 'unknown')
                
                if status != last_status:
                    elapsed = time.time() - process_start
                    logger.info(f"   Status: {last_status} → {status} (at {elapsed:.1f}s)")
                    last_status = status
                
                if status == 'completed':
                    process_time = time.time() - process_start
                    
                    # 처리 정보
                    info = doc.get('processing_info', {})
                    actual_chunks = info.get('total_chunks', 0)
                    embeddings = info.get('embeddings_created', 0)
                    
                    logger.info(f"\n✅ Processing completed:")
                    logger.info(f"   - Processing time: {process_time:.2f}s")
                    logger.info(f"   - Actual chunks: {actual_chunks}")
                    logger.info(f"   - Embeddings created: {embeddings}")
                    
                    # 첫 번째 청크에서 상세 정보 확인
                    chunk = await self.mongo.db.pdf_chunks.find_one(
                        {"document_id": document_id}
                    )
                    
                    if chunk and 'chunk_metadata' in chunk:
                        meta = chunk['chunk_metadata']
                        logger.info(f"\n📊 Processing details:")
                        logger.info(f"   - Chunking strategy: {meta.get('chunking_strategy')}")
                        logger.info(f"   - Embedding model: {meta.get('model', 'N/A')}")
                    
                    # 로그 분석
                    log_timings = await self.analyze_processing_logs(document_id)
                    if log_timings:
                        logger.info(f"\n⏱️  Detailed timings from logs:")
                        for step, duration in log_timings.items():
                            logger.info(f"   - {step}: {duration:.2f}s")
                    
                    # 결과 반환
                    return {
                        'expected_chunks': chunks,
                        'actual_chunks': actual_chunks,
                        'embeddings': embeddings,
                        'upload_time': upload_time,
                        'process_time': process_time,
                        'total_time': upload_time + process_time,
                        'file_size': file_size,
                        'log_timings': log_timings
                    }
                
                elif status == 'failed':
                    logger.error(f"Processing failed: {doc.get('error_message')}")
                    break
            
            if time.time() - process_start > 60:
                logger.warning("Timeout after 60s")
                break
            
            await asyncio.sleep(1)
        
        return None
    
    async def run_timing_test(self):
        """타이밍 테스트 실행"""
        # 다양한 크기 테스트
        test_sizes = [5, 10, 20, 50]
        results = []
        
        for size in test_sizes:
            result = await self.test_single_pdf(size)
            if result:
                results.append(result)
            await asyncio.sleep(2)  # 테스트 간 간격
        
        # 결과 분석
        if results:
            logger.info(f"\n{'='*60}")
            logger.info("📊 Timing Test Summary")
            logger.info(f"{'='*60}")
            
            logger.info("\n| Chunks | Upload | Process | Total | Embeddings | File Size |")
            logger.info("|--------|--------|---------|-------|------------|-----------|")
            
            for r in results:
                logger.info(
                    f"| {r['actual_chunks']:6} | {r['upload_time']:6.2f}s | {r['process_time']:7.2f}s | "
                    f"{r['total_time']:5.2f}s | {r['embeddings']:10} | {r['file_size']:9,} |"
                )
            
            # 임베딩 시간 분석
            logger.info("\n📈 Processing Time Breakdown:")
            for i, r in enumerate(results):
                logger.info(f"\n{r['actual_chunks']} chunks:")
                logger.info(f"  - Total processing: {r['process_time']:.2f}s")
                
                if r.get('log_timings'):
                    timings = r['log_timings']
                    if 'embedding' in timings:
                        embed_pct = (timings['embedding'] / r['process_time']) * 100
                        logger.info(f"  - Embedding time: {timings['embedding']:.2f}s ({embed_pct:.1f}%)")
                    if 'text_extraction' in timings:
                        logger.info(f"  - Text extraction: {timings['text_extraction']:.2f}s")
                    if 'chunking' in timings:
                        logger.info(f"  - Chunking: {timings['chunking']:.2f}s")
                
                # 청크당 평균 시간
                if r['actual_chunks'] > 0:
                    avg_per_chunk = r['process_time'] / r['actual_chunks']
                    logger.info(f"  - Average per chunk: {avg_per_chunk:.3f}s")
        
        # 정리
        test_dir = Path("test_timing")
        if test_dir.exists():
            import shutil
            shutil.rmtree(test_dir)
            logger.info("\n🧹 Cleaned up test files")
    
    async def check_embedding_config(self):
        """임베딩 설정 확인"""
        logger.info("\n🔧 Embedding Configuration:")
        logger.info(f"  - Model: {settings.OPENAI_EMBEDDING_MODEL}")
        logger.info(f"  - Batch size: {settings.PDF_BATCH_SIZE}")
        logger.info(f"  - Sub-batch size: {settings.PDF_SUB_BATCH_SIZE}")
        logger.info(f"  - Max concurrent API calls: {settings.PDF_MAX_CONCURRENT_API_CALLS}")


async def main():
    """메인 함수"""
    # API 서버 확인
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{settings.API_PORT}/health")
            if response.status_code != 200:
                logger.error("❌ API server not running")
                return
            logger.info("✅ API server is running")
    except:
        logger.error("❌ Cannot connect to API server")
        return
    
    # 로깅 모드 확인
    logger.info(f"\n📝 Processing log mode: {settings.PROCESSING_LOG_MODE}")
    if settings.PROCESSING_LOG_MODE not in ["file", "both"]:
        logger.warning("⚠️  Detailed timing logs only available with file logging enabled")
        logger.info("   Set PROCESSING_LOG_MODE=both in .env.development for detailed timings")
    
    tester = DetailedTimingTester()
    
    # 임베딩 설정 확인
    await tester.check_embedding_config()
    
    # 타이밍 테스트 실행
    await tester.run_timing_test()


if __name__ == "__main__":
    asyncio.run(main())
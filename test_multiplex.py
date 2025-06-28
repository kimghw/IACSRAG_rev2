# test_multiplex.py
"""멀티플렉싱(동시 처리) 테스트"""

import asyncio
import logging
import sys
import os
import time
from datetime import datetime
from pathlib import Path
import httpx
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.core.config import settings
from infra.databases.mongo_db import MongoDB

# 로깅 설정 - 스레드 정보 포함
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)-10s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MultiplexTester:
    """멀티플렉싱 테스터"""
    
    def __init__(self):
        self.api_url = f"http://localhost:{settings.API_PORT}"
        self.mongo = MongoDB()
        self.results = {}
        self.start_time = None
        
    def create_test_pdf(self, name: str, size: int) -> Path:
        """테스트 PDF 생성"""
        try:
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            from reportlab.lib.units import inch
        except ImportError:
            logger.error("reportlab not installed")
            return None
        
        test_dir = Path("test_multiplex")
        test_dir.mkdir(exist_ok=True)
        pdf_path = test_dir / f"{name}.pdf"
        
        c = canvas.Canvas(str(pdf_path), pagesize=letter)
        width, height = letter
        
        # 제목
        c.setFont("Helvetica-Bold", 20)
        c.drawString(inch, height - inch, f"Multiplex Test - {name}")
        c.drawString(inch, height - 1.5*inch, f"Size: {size} sections")
        
        # 내용 생성 (크기에 따라)
        c.setFont("Helvetica", 11)
        y = height - 2.5*inch
        
        for section in range(size):
            if y < inch:
                c.showPage()
                c.setFont("Helvetica", 11)
                y = height - inch
            
            c.setFont("Helvetica-Bold", 14)
            c.drawString(inch, y, f"Section {section + 1}")
            y -= 20
            
            c.setFont("Helvetica", 11)
            # 각 섹션에 여러 단락 추가
            for para in range(5):
                text = f"This is paragraph {para + 1} of section {section + 1}. " * 3
                # 긴 텍스트 줄바꿈
                words = text.split()
                line = ""
                for word in words:
                    if len(line + word) > 70:
                        c.drawString(inch, y, line)
                        y -= 15
                        line = word + " "
                    else:
                        line += word + " "
                if line:
                    c.drawString(inch, y, line)
                    y -= 15
            y -= 10
        
        c.save()
        return pdf_path
    
    async def upload_pdf_with_timing(self, pdf_path: Path, pdf_id: str) -> Dict[str, Any]:
        """PDF 업로드 및 시간 측정"""
        start = time.time()
        
        logger.info(f"📤 [{pdf_id}] Starting upload: {pdf_path.name}")
        
        with open(pdf_path, 'rb') as f:
            pdf_content = f.read()
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            files = {'file': (pdf_path.name, pdf_content, 'application/pdf')}
            
            response = await client.post(
                f"{self.api_url}/api/v1/upload/pdf",
                files=files
            )
            
            upload_time = time.time() - start
            
            if response.status_code == 200:
                result = response.json()
                document_id = result['document_id']
                
                logger.info(f"✅ [{pdf_id}] Upload complete in {upload_time:.2f}s - Doc ID: {document_id[:8]}...")
                
                return {
                    'pdf_id': pdf_id,
                    'document_id': document_id,
                    'upload_time': upload_time,
                    'file_size': pdf_path.stat().st_size,
                    'status': 'uploaded'
                }
            else:
                logger.error(f"❌ [{pdf_id}] Upload failed: {response.status_code}")
                return None
    
    async def monitor_processing(self, doc_info: Dict[str, Any]) -> Dict[str, Any]:
        """처리 상태 모니터링"""
        pdf_id = doc_info['pdf_id']
        document_id = doc_info['document_id']
        
        logger.info(f"⏳ [{pdf_id}] Monitoring processing...")
        
        start = time.time()
        last_status = None
        
        while time.time() - start < 60:  # 최대 60초
            doc = await self.mongo.find_one("uploads", {"document_id": document_id})
            
            if doc:
                status = doc.get('status', 'unknown')
                
                if status != last_status:
                    elapsed = time.time() - start
                    logger.info(f"   [{pdf_id}] Status: {last_status} → {status} (at {elapsed:.1f}s)")
                    last_status = status
                
                if status == 'completed':
                    processing_time = time.time() - start
                    
                    info = doc.get('processing_info', {})
                    chunks = info.get('total_chunks', 0)
                    embeddings = info.get('embeddings_created', 0)
                    
                    logger.info(f"✅ [{pdf_id}] Completed in {processing_time:.2f}s - {chunks} chunks, {embeddings} embeddings")
                    
                    return {
                        **doc_info,
                        'processing_time': processing_time,
                        'total_time': doc_info['upload_time'] + processing_time,
                        'chunks': chunks,
                        'embeddings': embeddings,
                        'status': 'completed'
                    }
                
                elif status == 'failed':
                    logger.error(f"❌ [{pdf_id}] Processing failed: {doc.get('error_message')}")
                    return {**doc_info, 'status': 'failed'}
            
            await asyncio.sleep(1)
        
        logger.warning(f"⏱️  [{pdf_id}] Timeout after 60s")
        return {**doc_info, 'status': 'timeout'}
    
    async def run_multiplex_test(self, pdf_count: int = 5):
        """멀티플렉싱 테스트 실행"""
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 Multiplexing Test - {pdf_count} PDFs")
        logger.info(f"{'='*60}")
        logger.info(f"\nConfiguration:")
        logger.info(f"  - Max Concurrent PDFs: {settings.PDF_MAX_CONCURRENT_PROCESSING}")
        logger.info(f"  - Max Concurrent API Calls: {settings.PDF_MAX_CONCURRENT_API_CALLS}")
        logger.info(f"  - Batch Size: {settings.PDF_BATCH_SIZE}")
        logger.info(f"  - Sub-batch Size: {settings.PDF_SUB_BATCH_SIZE}")
        
        self.start_time = time.time()
        
        # 1. 다양한 크기의 PDF 생성
        logger.info(f"\n📄 Creating {pdf_count} test PDFs...")
        pdf_files = []
        sizes = [5, 10, 15, 20, 25]  # 섹션 수
        
        for i in range(pdf_count):
            size = sizes[i % len(sizes)]
            pdf_path = self.create_test_pdf(f"multiplex_{i+1}", size)
            if pdf_path:
                pdf_files.append((pdf_path, f"PDF{i+1}", size))
                logger.info(f"   Created: {pdf_path.name} ({size} sections)")
        
        # 2. 동시 업로드
        logger.info(f"\n📤 Starting concurrent uploads...")
        upload_tasks = []
        
        for pdf_path, pdf_id, size in pdf_files:
            task = self.upload_pdf_with_timing(pdf_path, pdf_id)
            upload_tasks.append(task)
        
        # 모든 업로드 동시 실행
        upload_results = await asyncio.gather(*upload_tasks)
        successful_uploads = [r for r in upload_results if r is not None]
        
        logger.info(f"\n✅ Uploads complete: {len(successful_uploads)}/{pdf_count} successful")
        
        # 3. 처리 모니터링 (동시)
        logger.info(f"\n⏳ Monitoring all processing concurrently...")
        monitor_tasks = []
        
        for doc_info in successful_uploads:
            task = self.monitor_processing(doc_info)
            monitor_tasks.append(task)
        
        # 모든 처리 모니터링 동시 실행
        final_results = await asyncio.gather(*monitor_tasks)
        
        # 4. 결과 분석
        await self.analyze_results(final_results)
        
        # 정리
        await self.cleanup()
    
    async def analyze_results(self, results: List[Dict[str, Any]]):
        """결과 분석 및 출력"""
        total_time = time.time() - self.start_time
        
        logger.info(f"\n{'='*60}")
        logger.info("📊 Multiplexing Test Results")
        logger.info(f"{'='*60}")
        
        # 개별 결과
        logger.info("\nIndividual Results:")
        for r in results:
            if r['status'] == 'completed':
                logger.info(f"  {r['pdf_id']}:")
                logger.info(f"    - Upload: {r['upload_time']:.2f}s")
                logger.info(f"    - Processing: {r['processing_time']:.2f}s")
                logger.info(f"    - Total: {r['total_time']:.2f}s")
                logger.info(f"    - Chunks: {r['chunks']}, Embeddings: {r['embeddings']}")
        
        # 통계
        completed = [r for r in results if r['status'] == 'completed']
        if completed:
            avg_upload = sum(r['upload_time'] for r in completed) / len(completed)
            avg_processing = sum(r['processing_time'] for r in completed) / len(completed)
            avg_total = sum(r['total_time'] for r in completed) / len(completed)
            total_chunks = sum(r['chunks'] for r in completed)
            total_embeddings = sum(r['embeddings'] for r in completed)
            
            logger.info("\n📈 Statistics:")
            logger.info(f"  - Total execution time: {total_time:.2f}s")
            logger.info(f"  - Average upload time: {avg_upload:.2f}s")
            logger.info(f"  - Average processing time: {avg_processing:.2f}s")
            logger.info(f"  - Average total time per PDF: {avg_total:.2f}s")
            logger.info(f"  - Total chunks processed: {total_chunks}")
            logger.info(f"  - Total embeddings created: {total_embeddings}")
            
            # 효율성 계산
            sequential_time = sum(r['total_time'] for r in completed)
            efficiency = (sequential_time / total_time - 1) * 100
            logger.info(f"\n🚀 Multiplexing Efficiency:")
            logger.info(f"  - Sequential time (if processed one by one): {sequential_time:.2f}s")
            logger.info(f"  - Actual parallel time: {total_time:.2f}s")
            logger.info(f"  - Speed improvement: {efficiency:.1f}%")
        
        logger.info(f"\n{'='*60}")
    
    async def cleanup(self):
        """테스트 파일 정리"""
        test_dir = Path("test_multiplex")
        if test_dir.exists():
            import shutil
            shutil.rmtree(test_dir)
            logger.info("🧹 Cleaned up test files")
    
    async def run_stress_test(self, waves: int = 3, pdfs_per_wave: int = 5):
        """스트레스 테스트 - 여러 웨이브로 연속 처리"""
        logger.info(f"\n{'='*60}")
        logger.info(f"🔥 Stress Test - {waves} waves × {pdfs_per_wave} PDFs")
        logger.info(f"{'='*60}")
        
        all_results = []
        
        for wave in range(waves):
            logger.info(f"\n🌊 Wave {wave + 1}/{waves}")
            await self.run_multiplex_test(pdfs_per_wave)
            all_results.append(self.results)
            
            if wave < waves - 1:
                logger.info("\n⏸️  Waiting 5 seconds before next wave...")
                await asyncio.sleep(5)
        
        logger.info(f"\n🏁 Stress test completed!")


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
    
    tester = MultiplexTester()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "stress":
            # 스트레스 테스트
            waves = int(sys.argv[2]) if len(sys.argv) > 2 else 3
            pdfs = int(sys.argv[3]) if len(sys.argv) > 3 else 5
            await tester.run_stress_test(waves, pdfs)
        else:
            # 일반 멀티플렉싱 테스트
            count = int(sys.argv[1])
            await tester.run_multiplex_test(count)
    else:
        # 기본: 5개 PDF
        await tester.run_multiplex_test(5)


if __name__ == "__main__":
    asyncio.run(main())
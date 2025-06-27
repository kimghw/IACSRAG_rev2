# test_pdf_upload.py
import asyncio
import logging
import sys
import os
from datetime import datetime, timezone
from pathlib import Path
import json

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 프로젝트 모듈 import
from upload_service.orchestrator import UploadOrchestrator
from upload_service.schema import UploadRequest
from infra.databases.mongo_db import MongoDB
from aiokafka import AIOKafkaConsumer
from infra.core.config import settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PdfUploadTester:
    """PDF 업로드 및 이벤트 모니터링 테스터"""
    
    def __init__(self):
        self.orchestrator = UploadOrchestrator()
        self.mongo = MongoDB()
        self.test_pdf_path = None
    
    async def create_test_pdf(self):
        """테스트용 PDF 파일 생성"""
        try:
            # PyPDF2 사용하여 간단한 PDF 생성
            from PyPDF2 import PdfWriter, PdfReader
            from reportlab.pdfgen import canvas
            from reportlab.lib.pagesizes import letter
            import tempfile
            
            # 임시 PDF 생성
            with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
                self.test_pdf_path = tmp.name
                
            # ReportLab으로 PDF 생성
            c = canvas.Canvas(self.test_pdf_path, pagesize=letter)
            c.drawString(100, 750, "Test PDF for IACSRAG System")
            c.drawString(100, 700, f"Created at: {datetime.now()}")
            c.drawString(100, 650, "This is a test document for vector embedding.")
            c.drawString(100, 600, "It contains sample text to test the PDF processing pipeline.")
            c.drawString(100, 550, "The system should extract this text, chunk it, and create embeddings.")
            c.save()
            
            logger.info(f"✅ Created test PDF: {self.test_pdf_path}")
            return True
            
        except ImportError:
            logger.warning("ReportLab not installed. Using a pre-existing PDF file.")
            # 기존 PDF 파일 사용
            return False
    
    async def use_existing_pdf(self, pdf_path: str):
        """기존 PDF 파일 사용"""
        if not os.path.exists(pdf_path):
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        
        self.test_pdf_path = pdf_path
        logger.info(f"Using existing PDF: {pdf_path}")
    
    async def upload_pdf(self):
        """PDF 업로드 실행"""
        if not self.test_pdf_path:
            raise ValueError("No PDF file specified")
        
        # 파일 읽기
        with open(self.test_pdf_path, 'rb') as f:
            file_content = f.read()
        
        file_name = os.path.basename(self.test_pdf_path)
        file_size = len(file_content)
        
        # UploadRequest 생성
        upload_request = UploadRequest(
            document_id=f"test-{datetime.now().strftime('%Y%m%d%H%M%S')}",
            filename=file_name,
            content_type="application/pdf",
            file_size=file_size,
            metadata={
                "test": True,
                "source": "test_script",
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            uploaded_at=datetime.now(timezone.utc)
        )
        
        logger.info(f"\n📤 Uploading PDF:")
        logger.info(f"   Document ID: {upload_request.document_id}")
        logger.info(f"   Filename: {upload_request.filename}")
        logger.info(f"   Size: {file_size:,} bytes")
        
        # Orchestrator를 통해 업로드
        try:
            result = await self.orchestrator.process_upload(upload_request, file_content)
            logger.info(f"\n✅ Upload successful!")
            logger.info(f"   Status: {result.status}")
            logger.info(f"   Message: {result.message}")
            return upload_request.document_id
            
        except Exception as e:
            logger.error(f"❌ Upload failed: {e}")
            raise
    
    async def monitor_kafka_events(self, document_id: str, timeout: int = 30):
        """Kafka 이벤트 모니터링"""
        logger.info(f"\n🔍 Monitoring Kafka events for document: {document_id}")
        logger.info(f"   Timeout: {timeout} seconds")
        
        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"test-monitor-{datetime.now().timestamp()}",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        await consumer.start()
        
        try:
            # 타임아웃 설정
            end_time = asyncio.get_event_loop().time() + timeout
            event_found = False
            
            while asyncio.get_event_loop().time() < end_time:
                # 1초 타임아웃으로 메시지 대기
                try:
                    records = await asyncio.wait_for(
                        consumer.getmany(timeout_ms=1000),
                        timeout=1.5
                    )
                    
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            if msg.value.get('document_id') == document_id:
                                event_found = True
                                logger.info(f"\n📨 Event received!")
                                logger.info(f"   Topic: {msg.topic}")
                                logger.info(f"   Partition: {msg.partition}")
                                logger.info(f"   Offset: {msg.offset}")
                                logger.info(f"   Event data:")
                                for key, value in msg.value.items():
                                    logger.info(f"     - {key}: {value}")
                                
                except asyncio.TimeoutError:
                    continue
            
            if not event_found:
                logger.warning(f"⏱️  No event found for document {document_id} within {timeout} seconds")
            
        finally:
            await consumer.stop()
    
    async def check_database_status(self, document_id: str):
        """데이터베이스 상태 확인"""
        logger.info(f"\n📊 Checking database status for document: {document_id}")
        
        # MongoDB 확인
        doc = await self.mongo.find_one("uploads", {"document_id": document_id})
        if doc:
            logger.info(f"\n✅ Document found in MongoDB:")
            logger.info(f"   Status: {doc.get('status')}")
            logger.info(f"   GridFS File ID: {doc.get('gridfs_file_id')}")
            logger.info(f"   Uploaded at: {doc.get('uploaded_at')}")
            
            # 처리 상태 추가 확인
            if doc.get('status') == 'completed':
                if 'processed_info' in doc:
                    info = doc['processed_info']
                    logger.info(f"   Processing info:")
                    logger.info(f"     - Total chunks: {info.get('total_chunks', 0)}")
                    logger.info(f"     - Total embeddings: {info.get('total_embeddings', 0)}")
        else:
            logger.warning(f"❌ Document not found in MongoDB")
    
    async def run_full_test(self, pdf_path: str = None):
        """전체 테스트 실행"""
        logger.info("🚀 Starting PDF Upload Test")
        logger.info("="*60)
        
        try:
            # 1. PDF 준비
            if pdf_path:
                await self.use_existing_pdf(pdf_path)
            else:
                created = await self.create_test_pdf()
                if not created:
                    logger.error("Please provide a PDF file path")
                    return
            
            # 2. PDF 업로드
            document_id = await self.upload_pdf()
            
            # 3. 이벤트 모니터링 (별도 태스크로 실행)
            monitor_task = asyncio.create_task(
                self.monitor_kafka_events(document_id, timeout=10)
            )
            
            # 4. 잠시 대기 후 DB 상태 확인
            await asyncio.sleep(2)
            await self.check_database_status(document_id)
            
            # 5. 모니터링 완료 대기
            await monitor_task
            
            # 6. 처리 완료 대기 (선택사항)
            logger.info(f"\n⏳ Waiting for processing to complete...")
            for i in range(30):  # 최대 30초 대기
                await asyncio.sleep(1)
                doc = await self.mongo.find_one("uploads", {"document_id": document_id})
                if doc and doc.get('status') == 'completed':
                    logger.info(f"\n✅ Processing completed!")
                    await self.check_database_status(document_id)
                    break
                elif doc and doc.get('status') == 'failed':
                    logger.error(f"\n❌ Processing failed: {doc.get('error_message')}")
                    break
            
            logger.info("\n" + "="*60)
            logger.info("📋 Test Summary:")
            logger.info(f"   Document ID: {document_id}")
            logger.info(f"   PDF Path: {self.test_pdf_path}")
            logger.info("   Check Kafka UI: http://localhost:8080")
            logger.info("   Check Qdrant: http://localhost:6333/dashboard")
            
        except Exception as e:
            logger.error(f"\n❌ Test failed: {e}")
            raise
        
        finally:
            # 테스트 PDF 정리 (생성한 경우)
            if self.test_pdf_path and self.test_pdf_path.startswith('/tmp/'):
                try:
                    os.remove(self.test_pdf_path)
                    logger.info(f"🧹 Cleaned up test PDF")
                except:
                    pass

async def main():
    """메인 함수"""
    # 명령행 인자 확인
    pdf_path = None
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
    
    # 서버가 실행 중인지 확인
    try:
        import httpx
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{settings.API_PORT}/health")
            if response.status_code != 200:
                logger.warning("⚠️  API server is not running. Please start it first:")
                logger.warning("   python main.py")
                return
    except:
        logger.error("❌ API server is not running. Please start it first:")
        logger.error("   python main.py")
        return
    
    # 테스트 실행
    tester = PdfUploadTester()
    await tester.run_full_test(pdf_path)

if __name__ == "__main__":
    # ReportLab 설치 안내
    try:
        import reportlab
    except ImportError:
        logger.info("\n💡 Tip: Install reportlab to auto-generate test PDFs:")
        logger.info("   pip install reportlab")
        logger.info("   Or provide your own PDF: python test_pdf_upload.py /path/to/your.pdf\n")
    
    asyncio.run(main())
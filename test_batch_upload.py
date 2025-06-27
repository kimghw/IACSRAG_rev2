# test_batch_upload.py
import asyncio
import logging
import sys
import os
import httpx
import json
from pathlib import Path
from datetime import datetime

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infra.core.config import settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class BatchUploadTester:
    """배치 업로드 테스터"""
    
    def __init__(self):
        self.api_url = f"http://localhost:{settings.API_PORT}"
        self.test_files = []
    
    def create_test_files(self):
        """다양한 타입의 테스트 파일 생성"""
        test_dir = Path("test_files")
        test_dir.mkdir(exist_ok=True)
        
        # 1. PDF 파일 생성 (텍스트 파일로 대체)
        pdf_content = """Test PDF Document
This is a test PDF document for batch upload testing.
It contains multiple lines of text.
The system should process this as a PDF file.
Created at: {}""".format(datetime.now())
        
        pdf_path = test_dir / "test_document.pdf"
        pdf_path.write_text(pdf_content)
        self.test_files.append(("test_document.pdf", pdf_content))
        
        # 2. Markdown 파일 생성
        md_content = """# Test Markdown Document

## Introduction
This is a test markdown document for batch upload testing.

### Features
- Bullet point 1
- Bullet point 2
- Bullet point 3

### Code Example
```python
def hello_world():
    print("Hello, World!")
```

Created at: {}""".format(datetime.now())
        
        md_path = test_dir / "test_document.md"
        md_path.write_text(md_content)
        self.test_files.append(("test_document.md", md_content))
        
        # 3. JSON 파일 생성
        json_data = {
            "title": "Test JSON Document",
            "description": "This is a test JSON document for batch upload testing",
            "data": {
                "items": ["item1", "item2", "item3"],
                "count": 3,
                "active": True
            },
            "created_at": datetime.now().isoformat()
        }
        json_content = json.dumps(json_data, indent=2)
        
        json_path = test_dir / "test_document.json"
        json_path.write_text(json_content)
        self.test_files.append(("test_document.json", json_content))
        
        # 4. 추가 파일들 (배치 테스트용)
        for i in range(2):
            # 추가 PDF
            pdf_content_extra = f"Extra PDF Document {i+1}\nContent for testing batch upload\nFile number: {i+1}"
            pdf_path_extra = test_dir / f"extra_document_{i+1}.pdf"
            pdf_path_extra.write_text(pdf_content_extra)
            self.test_files.append((f"extra_document_{i+1}.pdf", pdf_content_extra))
            
            # 추가 Markdown
            md_content_extra = f"# Extra Markdown {i+1}\n\nContent for testing batch upload\n\nFile number: {i+1}"
            md_path_extra = test_dir / f"extra_document_{i+1}.md"
            md_path_extra.write_text(md_content_extra)
            self.test_files.append((f"extra_document_{i+1}.md", md_content_extra))
        
        logger.info(f"Created {len(self.test_files)} test files")
    
    async def test_batch_upload(self):
        """배치 업로드 테스트"""
        logger.info("\n" + "="*60)
        logger.info("🚀 Starting Batch Upload Test")
        logger.info("="*60)
        
        # 메타데이터 준비
        metadata = {
            "test_run": True,
            "test_timestamp": datetime.now().isoformat(),
            "source": "batch_upload_test"
        }
        
        # 파일 준비
        files = []
        for filename, content in self.test_files:
            files.append(('files', (filename, content.encode('utf-8'))))
        
        # 배치 업로드 실행
        async with httpx.AsyncClient(timeout=30.0) as client:
            logger.info(f"\n📤 Uploading {len(self.test_files)} files in batch...")
            
            response = await client.post(
                f"{self.api_url}/api/v1/upload/batch",
                files=files,
                data={'metadata': json.dumps(metadata)}
            )
            
            if response.status_code == 200:
                result = response.json()
                
                logger.info("\n✅ Batch Upload Successful!")
                logger.info(f"   Total Files: {result['total_files']}")
                logger.info(f"   Successful: {result['successful_count']}")
                logger.info(f"   Failed: {result['failed_count']}")
                
                # 성공한 업로드 정보
                if result['successful_uploads']:
                    logger.info("\n📋 Successful Uploads:")
                    for upload in result['successful_uploads']:
                        logger.info(f"   - {upload['filename']} (ID: {upload['document_id'][:8]}...)")
                
                # 실패한 업로드 정보
                if result['failed_uploads']:
                    logger.info("\n❌ Failed Uploads:")
                    for upload in result['failed_uploads']:
                        logger.info(f"   - {upload['filename']}: {upload['error']}")
                
                return result['successful_uploads']
            else:
                logger.error(f"❌ Batch upload failed: {response.status_code}")
                logger.error(f"   Response: {response.text}")
                return None
    
    async def monitor_kafka_events(self, document_ids: list, duration: int = 10):
        """Kafka 이벤트 모니터링"""
        logger.info(f"\n🔍 Monitoring Kafka events for {len(document_ids)} documents")
        
        from aiokafka import AIOKafkaConsumer
        
        consumer = AIOKafkaConsumer(
            settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
            group_id=f"test-batch-monitor-{datetime.now().timestamp()}",
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )
        
        await consumer.start()
        
        try:
            end_time = asyncio.get_event_loop().time() + duration
            event_count = 0
            event_types = {}
            
            while asyncio.get_event_loop().time() < end_time:
                try:
                    records = await asyncio.wait_for(
                        consumer.getmany(timeout_ms=1000),
                        timeout=1.5
                    )
                    
                    for topic_partition, messages in records.items():
                        for msg in messages:
                            doc_id = msg.value.get('document_id')
                            if doc_id in [doc['document_id'] for doc in document_ids]:
                                event_count += 1
                                event_type = msg.value.get('event_type', 'unknown')
                                event_types[event_type] = event_types.get(event_type, 0) + 1
                                
                                logger.info(f"\n📨 Event #{event_count} received:")
                                logger.info(f"   Type: {event_type}")
                                logger.info(f"   Document: {doc_id[:8]}...")
                                logger.info(f"   Filename: {msg.value.get('filename')}")
                                
                except asyncio.TimeoutError:
                    continue
            
            logger.info(f"\n📊 Event Summary:")
            logger.info(f"   Total Events: {event_count}")
            for event_type, count in event_types.items():
                logger.info(f"   - {event_type}: {count}")
                
        finally:
            await consumer.stop()
    
    async def run_full_test(self):
        """전체 테스트 실행"""
        try:
            # 1. 테스트 파일 생성
            self.create_test_files()
            
            # 2. 배치 업로드 실행
            successful_uploads = await self.test_batch_upload()
            
            if successful_uploads:
                # 3. Kafka 이벤트 모니터링
                await self.monitor_kafka_events(successful_uploads, duration=10)
            
            logger.info("\n" + "="*60)
            logger.info("✅ Batch Upload Test Completed!")
            logger.info("="*60)
            
        finally:
            # 테스트 파일 정리
            self.cleanup_test_files()
    
    def cleanup_test_files(self):
        """테스트 파일 정리"""
        test_dir = Path("test_files")
        if test_dir.exists():
            import shutil
            shutil.rmtree(test_dir)
            logger.info("🧹 Cleaned up test files")

async def test_simple_batch():
    """간단한 배치 업로드 테스트"""
    logger.info("\n🚀 Simple Batch Upload Test")
    
    # 간단한 파일 3개 준비
    files = [
        ('files', ('simple1.pdf', b'Simple PDF content 1')),
        ('files', ('simple2.md', b'# Simple Markdown\n\nContent 2')),
        ('files', ('simple3.json', b'{"test": true, "value": 3}'))
    ]
    
    metadata = {
        "test": "simple_batch",
        "timestamp": datetime.now().isoformat()
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"http://localhost:{settings.API_PORT}/api/v1/upload/batch",
            files=files,
            data={'metadata': json.dumps(metadata)}
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Success! Total files: {result['total_files']}")
            logger.info(f"   Uploaded: {result['successful_count']}")
            logger.info(f"   Failed: {result['failed_count']}")
            
            # 개별 결과 출력
            for upload in result['successful_uploads']:
                logger.info(f"   ✓ {upload['filename']} → {upload['document_id'][:8]}...")
        else:
            logger.error(f"❌ Failed: {response.status_code}")
            logger.error(f"   {response.text}")

async def main():
    """메인 함수"""
    # API 서버 확인
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"http://localhost:{settings.API_PORT}/health")
            if response.status_code != 200:
                logger.error("⚠️  API server is not running. Please start it first.")
                return
    except:
        logger.error("❌ API server is not running. Please start it first:")
        logger.error("   python main.py")
        return
    
    # 테스트 선택
    if len(sys.argv) > 1 and sys.argv[1] == "simple":
        await test_simple_batch()
    else:
        # 전체 테스트 실행
        tester = BatchUploadTester()
        await tester.run_full_test()

if __name__ == "__main__":
    asyncio.run(main())
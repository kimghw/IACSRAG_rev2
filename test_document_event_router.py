# test_document_event_router.py
import asyncio
import logging
import sys
import os
from datetime import datetime, timezone
import httpx
import json

# 프로젝트 루트를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from infra.events.document_event_router import DocumentEventRouter
from schema import DocumentEventType
from infra.core.config import settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 테스트 핸들러들
async def handle_pdf_event(event_data: dict):
    """PDF 이벤트 핸들러"""
    logger.info(f"📄 PDF Handler received event:")
    logger.info(f"   Document ID: {event_data.get('document_id')}")
    logger.info(f"   Filename: {event_data.get('filename')}")
    logger.info(f"   Size: {event_data.get('file_size'):,} bytes")

async def handle_markdown_event(event_data: dict):
    """Markdown 이벤트 핸들러"""
    logger.info(f"📝 Markdown Handler received event:")
    logger.info(f"   Document ID: {event_data.get('document_id')}")
    logger.info(f"   Filename: {event_data.get('filename')}")
    logger.info(f"   Size: {event_data.get('file_size'):,} bytes")

async def handle_json_event(event_data: dict):
    """JSON 이벤트 핸들러"""
    logger.info(f"📊 JSON Handler received event:")
    logger.info(f"   Document ID: {event_data.get('document_id')}")
    logger.info(f"   Filename: {event_data.get('filename')}")
    logger.info(f"   Size: {event_data.get('file_size'):,} bytes")

async def test_file_upload(filename: str, content: str):
    """파일 업로드 테스트"""
    async with httpx.AsyncClient() as client:
        # 파일 내용을 bytes로 변환
        file_content = content.encode('utf-8')
        
        # 파일 업로드
        files = {'file': (filename, file_content)}
        
        response = await client.post(
            f"http://localhost:{settings.API_PORT}/api/v1/upload/file",
            files=files
        )
        
        if response.status_code == 200:
            result = response.json()
            logger.info(f"✅ Upload successful: {filename}")
            logger.info(f"   Response: {result}")
            return result
        else:
            logger.error(f"❌ Upload failed: {response.status_code}")
            logger.error(f"   Response: {response.text}")
            return None

async def test_document_event_router():
    """Document Event Router 테스트"""
    logger.info("🚀 Starting Document Event Router Test\n")
    
    # 1. Document Event Router 생성 및 핸들러 등록
    router = DocumentEventRouter()
    router.register_handler(DocumentEventType.PDF, handle_pdf_event)
    router.register_handler(DocumentEventType.MARKDOWN, handle_markdown_event)
    router.register_handler(DocumentEventType.JSON, handle_json_event)
    
    # 2. 라우터 시작 (백그라운드 태스크)
    router_task = asyncio.create_task(router.start())
    logger.info("✅ Document Event Router started\n")
    
    # 라우터가 준비될 때까지 잠시 대기
    await asyncio.sleep(2)
    
    try:
        # 3. 다양한 파일 타입 업로드 테스트
        test_files = [
            ("test_document.pdf", "This is a test PDF content"),
            ("README.md", "# Test Markdown\n\nThis is a test markdown file"),
            ("data.json", '{"test": true, "message": "Hello JSON"}')
        ]
        
        logger.info("📤 Uploading test files...\n")
        
        for filename, content in test_files:
            await test_file_upload(filename, content)
            await asyncio.sleep(1)  # 각 업로드 사이 대기
        
        # 4. 이벤트 처리 대기
        logger.info("\n⏳ Waiting for events to be processed...")
        await asyncio.sleep(5)
        
    finally:
        # 5. 라우터 중지
        logger.info("\n🛑 Stopping Document Event Router...")
        await router.stop()
        router_task.cancel()
        
        try:
            await router_task
        except asyncio.CancelledError:
            pass
    
    logger.info("\n✅ Test completed!")

async def test_supported_types():
    """지원 파일 타입 확인"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"http://localhost:{settings.API_PORT}/api/v1/upload/supported-types"
        )
        
        if response.status_code == 200:
            data = response.json()
            logger.info("\n📋 Supported file types:")
            for file_type in data['supported_types']:
                logger.info(f"   - {file_type['extension']}: {file_type['event_type']}")
                logger.info(f"     Content-Type: {file_type['content_type']}")
                logger.info(f"     Endpoint: {file_type['endpoint']}")
            logger.info(f"\n   Generic endpoint: {data['generic_endpoint']}")
            logger.info(f"   Max file size: {data['max_file_size']}")

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
    
    # 지원 파일 타입 확인
    await test_supported_types()
    
    # Document Event Router 테스트
    await test_document_event_router()

if __name__ == "__main__":
    asyncio.run(main())
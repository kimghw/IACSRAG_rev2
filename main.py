# main.py 수정
import asyncio
import sys  # 추가 필요
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import logging

from infra.core.config import settings
from infra.core.system_initializer import SystemInitializer  
from infra.api.upload_router import router as upload_router
from content_pdf.pdf_consumer import PdfConsumer

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 컨슈머 인스턴스
pdf_consumer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # 시작 시
    logger.info("Starting IACSRAG application...")
    
    # Kafka 컨슈머 시작
    global pdf_consumer
    pdf_consumer = PdfConsumer()
    asyncio.create_task(pdf_consumer.start())
    logger.info("PDF Consumer started")
    
    yield
    
    # 종료 시
    logger.info("Shutting down IACSRAG application...")
    if pdf_consumer:
        await pdf_consumer.stop()

# FastAPI 앱 생성
app = FastAPI(
    title="IACSRAG Content Processing System",
    version="2.0.0",
    lifespan=lifespan
)

# 라우터 등록
app.include_router(upload_router)  # prefix 제거 (router에 이미 정의됨)

@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "healthy", "service": "iacsrag"}

async def initialize_system():
    """시스템 초기화 명령"""
    initializer = SystemInitializer()
    success = await initializer.initialize_all()
    return success

if __name__ == "__main__":
    # 명령행 인자 확인
    if len(sys.argv) > 1 and sys.argv[1] == "init":
        # python main.py init 명령으로 초기화 실행
        success = asyncio.run(initialize_system())
        sys.exit(0 if success else 1)
    else:
        # 일반 서버 실행
        uvicorn.run(
            "main:app",
            host=settings.API_HOST,
            port=settings.API_PORT,
            reload=True
        )
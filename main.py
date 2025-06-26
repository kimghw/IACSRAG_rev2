# main.py
import asyncio
from fastapi import FastAPI
from contextlib import asynccontextmanager
import uvicorn
import logging

from infra.core.config import settings
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
app.include_router(upload_router, prefix="/api/v1")

@app.get("/health")
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "healthy", "service": "iacsrag"}

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=True
    )
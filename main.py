# main.py
import asyncio
import uvicorn
import sys
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from dotenv import load_dotenv
from datetime import datetime, timezone

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from infra.api.upload_router import router as upload_router
from infra.core.config import settings
from infra.events.event_producer import EventProducer
from infra.events.document_event_router import DocumentEventRouter
from infra.events.email_event_consumer import EmailEventConsumer
from infra.events.handlers import DOCUMENT_HANDLERS
from schema import DocumentEventType

# 로깅 설정
logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka 관련 라이브러리 로그 레벨 조정
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("aiokafka.consumer").setLevel(logging.WARNING)
logging.getLogger("aiokafka.conn").setLevel(logging.WARNING)
logging.getLogger("aiokafka.cluster").setLevel(logging.WARNING)

# 전역 변수
app_state = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    # 시작
    logger.info("🚀 Starting IACSRAG API Server...")
    
    # 이벤트 프로듀서 초기화
    producer = EventProducer()
    await producer._ensure_producer()
    
    # Document Event Router 생성
    document_router = DocumentEventRouter()
    
    # 핸들러 레지스트리에서 핸들러 등록
    for event_type, handler in DOCUMENT_HANDLERS.items():
        document_router.register_handler(event_type, handler)
        logger.info(f"📌 Registered handler for {event_type.value}")
    
    # Document Event Router 시작
    router_task = asyncio.create_task(document_router.start())
    app_state['document_router'] = document_router
    app_state['router_task'] = router_task
    
    # Email Event Consumer 시작
    email_consumer = EmailEventConsumer()
    email_task = asyncio.create_task(email_consumer.start())
    app_state['email_consumer'] = email_consumer
    app_state['email_task'] = email_task
    
    logger.info("✅ Document Event Router started")
    logger.info("✅ Email Event Consumer started")
    logger.info(f"📡 Listening on topics:")
    logger.info(f"   - {settings.KAFKA_TOPIC_DOCUMENT_UPLOADED}")
    logger.info(f"   - {settings.KAFKA_TOPIC_EMAIL_RECEIVED}")
    logger.info(f"🔧 Registered handlers: {[e.value for e in DOCUMENT_HANDLERS.keys()]}")
    
    yield
    
    # 종료
    logger.info("🛑 Shutting down IACSRAG API Server...")
    
    # Document Event Router 중지
    if 'document_router' in app_state:
        await app_state['document_router'].stop()
    
    if 'router_task' in app_state:
        app_state['router_task'].cancel()
        try:
            await app_state['router_task']
        except asyncio.CancelledError:
            pass
    
    # Email Event Consumer 중지
    if 'email_consumer' in app_state:
        await app_state['email_consumer'].stop()
    
    if 'email_task' in app_state:
        app_state['email_task'].cancel()
        try:
            await app_state['email_task']
        except asyncio.CancelledError:
            pass
    
    # 프로듀서 종료
    await EventProducer.shutdown()
    
    logger.info("👋 Server shutdown complete")

# FastAPI 앱 생성
app = FastAPI(
    title="IACSRAG Content Processing API",
    version="2.0.0",
    description="Event-driven content processing system with PDF, Markdown, JSON, and Email support",
    lifespan=lifespan
)

# 라우터 등록
app.include_router(upload_router)

@app.get("/")
async def root():
    """루트 엔드포인트"""
    return {
        "name": "IACSRAG Content Processing API",
        "version": "2.0.0",
        "status": "running",
        "endpoints": {
            "upload": "/api/v1/upload/file",
            "upload_pdf": "/api/v1/upload/pdf",
            "upload_markdown": "/api/v1/upload/markdown",
            "upload_json": "/api/v1/upload/json",
            "upload_batch": "/api/v1/upload/batch",
            "supported_types": "/api/v1/upload/supported-types",
            "health": "/health",
            "docs": "/docs"
        }
    }

@app.get("/health")
async def health_check():
    """헬스체크 엔드포인트"""
    # 등록된 핸들러 정보 추가
    registered_handlers = list(DOCUMENT_HANDLERS.keys()) if DOCUMENT_HANDLERS else []
    
    return {
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "services": {
            "api": "running",
            "document_router": "running" if app_state.get('router_task') and not app_state['router_task'].done() else "stopped",
            "email_consumer": "running" if app_state.get('email_task') and not app_state['email_task'].done() else "stopped",
            "registered_handlers": [h.value for h in registered_handlers]
        }
    }

if __name__ == "__main__":
    # 환경 설정 로드
    if len(sys.argv) > 1 and sys.argv[1] == "init":
        # 초기화 모드
        logger.info("🔧 Running system initialization...")
        from infra.core.system_initializer import SystemInitializer
        
        async def init():
            initializer = SystemInitializer()
            success = await initializer.initialize_all()
            if not success:
                sys.exit(1)
        
        asyncio.run(init())
        logger.info("✅ Initialization complete. You can now start the server.")
    else:
        # 서버 실행 모드
        env_file = '.env.development' if os.path.exists('.env.development') else '.env'
        load_dotenv(env_file)
        
        uvicorn.run(
            "main:app",
            host=settings.API_HOST,
            port=settings.API_PORT,
            reload=True,
            log_level="info"
        )
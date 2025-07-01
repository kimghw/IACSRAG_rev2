# infra/api/app.py
from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from infra.api.upload_router import router as upload_router
from infra.api.event_log_router import router as event_log_router

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """앱 시작/종료 시 실행되는 함수"""
    # 시작 시
    logger.info("FastAPI application starting...")
    yield
    # 종료 시
    logger.info("FastAPI application shutting down...")

def create_app() -> FastAPI:
    """FastAPI 앱 생성 팩토리"""
    app = FastAPI(
        title="IACSRAG Content Processing API",
        version="2.0.0",
        description="Event-driven content processing system",
        lifespan=lifespan
    )
    
    # 라우터 등록
    app.include_router(upload_router)
    app.include_router(event_log_router)  # 이벤트 로그 라우터 추가
    
    # 헬스체크 엔드포인트
    @app.get("/health")
    async def health_check():
        """서버 상태 확인"""
        return {
            "status": "healthy",
            "service": "IACSRAG API",
            "version": "2.0.0"
        }
    
    return app

# 개발용 앱 인스턴스
app = create_app()
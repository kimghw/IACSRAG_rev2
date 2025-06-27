from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
import os
import sys
from datetime import datetime
import logging
from typing import Optional
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from aiokafka import AIOKafkaProducer
import json
from bson import ObjectId
from pydantic import BaseModel
from pymongo import MongoClient
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
import tempfile
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from content_pdf.pdf_processor import PdfProcessor
from content_pdf.pdf_embedder import PdfEmbedder
from content_pdf.pdf_consumer import PdfConsumer

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 전역 변수
app_state = {}

class ProcessingStatus(BaseModel):
    id: str
    status: str
    progress: int
    message: Optional[str] = None
    chunks_count: Optional[int] = None
    embeddings_count: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 시작/종료 시 실행되는 함수"""
    # 시작 시
    try:
        # 환경 변수 로드
        app_state['mongodb_url'] = os.getenv('MONGODB_URL', 'mongodb://localhost:27017/')
        app_state['mongodb_db'] = os.getenv('MONGODB_DATABASE', 'pdf_processor')
        app_state['kafka_bootstrap_servers'] = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        app_state['qdrant_url'] = os.getenv('QDRANT_URL', 'localhost')
        qdrant_port_str = os.getenv('QDRANT_PORT', '6333')
        # 포트에서 콜론이 포함된 경우 처리 (예: "6333:6333")
        if ':' in qdrant_port_str:
            qdrant_port_str = qdrant_port_str.split(':')[0]
        app_state['qdrant_port'] = int(qdrant_port_str)
        
        # MongoDB 연결
        app_state['mongo_client'] = AsyncIOMotorClient(app_state['mongodb_url'])
        app_state['db'] = app_state['mongo_client'][app_state['mongodb_db']]
        
        # Kafka Producer 설정
        app_state['kafka_producer'] = AIOKafkaProducer(
            bootstrap_servers=app_state['kafka_bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await app_state['kafka_producer'].start()
        
        # Qdrant 연결 확인
        qdrant_url = app_state['qdrant_url']
        # URL에서 프로토콜 제거 (http:// 또는 https://)
        if qdrant_url.startswith('http://'):
            qdrant_url = qdrant_url[7:]
        elif qdrant_url.startswith('https://'):
            qdrant_url = qdrant_url[8:]
        
        # URL에서 포트 제거 (예: "localhost:6333")
        if ':' in qdrant_url:
            qdrant_url = qdrant_url.split(':')[0]
        
        app_state['qdrant_client'] = QdrantClient(
            host=qdrant_url,
            port=app_state['qdrant_port']
        )
        
        # PDF Consumer를 백그라운드 태스크로 시작
        consumer = PdfConsumer()
        app_state['consumer_task'] = asyncio.create_task(consumer.start())
        
        logger.info("FastAPI 서버가 시작되었습니다.")
        
    except Exception as e:
        logger.error(f"서버 시작 중 오류 발생: {e}")
        raise
    
    yield
    
    # 종료 시
    try:
        if 'kafka_producer' in app_state:
            await app_state['kafka_producer'].stop()
        
        if 'consumer_task' in app_state:
            app_state['consumer_task'].cancel()
            try:
                await app_state['consumer_task']
            except asyncio.CancelledError:
                pass
        
        if 'mongo_client' in app_state:
            app_state['mongo_client'].close()
        
        logger.info("FastAPI 서버가 종료되었습니다.")
        
    except Exception as e:
        logger.error(f"서버 종료 중 오류 발생: {e}")

# FastAPI 앱 생성
app = FastAPI(
    title="PDF Processing API",
    version="1.0.0",
    lifespan=lifespan
)

@app.get("/health")
async def health_check():
    """서버 상태 확인"""
    health_status = {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "services": {}
    }
    
    # MongoDB 상태 확인
    try:
        await app_state['db'].command('ping')
        health_status["services"]["mongodb"] = "connected"
    except Exception as e:
        health_status["services"]["mongodb"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    # Kafka 상태 확인
    try:
        if app_state['kafka_producer']._sender.sender_task and not app_state['kafka_producer']._sender.sender_task.done():
            health_status["services"]["kafka"] = "connected"
        else:
            health_status["services"]["kafka"] = "disconnected"
            health_status["status"] = "unhealthy"
    except Exception as e:
        health_status["services"]["kafka"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    # Qdrant 상태 확인
    try:
        app_state['qdrant_client'].get_collections()
        health_status["services"]["qdrant"] = "connected"
    except Exception as e:
        health_status["services"]["qdrant"] = f"error: {str(e)}"
        health_status["status"] = "unhealthy"
    
    return health_status

@app.post("/upload-pdf")
async def upload_pdf(file: UploadFile = File(...)):
    """PDF 파일 업로드 및 즉시 처리"""
    try:
        # 파일 확장자 확인
        if not file.filename.endswith('.pdf'):
            raise HTTPException(status_code=400, detail="PDF 파일만 업로드 가능합니다.")
        
        # 파일 내용 읽기
        file_content = await file.read()
        
        # 파일 크기 확인
        if len(file_content) > 50 * 1024 * 1024:  # 50MB 제한
            raise HTTPException(status_code=400, detail="파일 크기가 50MB를 초과합니다.")
        
        # 업로드 요청 생성
        from upload_service.orchestrator import UploadOrchestrator
        from upload_service.schema import UploadRequest
        from content_pdf.orchestrator import PdfOrchestrator
        from content_pdf.schema import PdfProcessingRequest
        import uuid
        
        upload_request = UploadRequest(
            document_id=str(uuid.uuid4()),
            filename=file.filename,
            content_type="application/pdf",
            file_size=len(file_content),
            metadata={},
            uploaded_at=datetime.now()
        )
        
        # 1. 파일 업로드 처리
        upload_orchestrator = UploadOrchestrator()
        upload_result = await upload_orchestrator.process_upload(upload_request, file_content)
        
        # 2. 즉시 PDF 처리 시작
        pdf_orchestrator = PdfOrchestrator()
        pdf_request = PdfProcessingRequest(
            document_id=upload_request.document_id,
            metadata=upload_request.metadata
        )
        
        # PDF 처리 실행 (텍스트 추출, 청킹, 임베딩 생성, 벡터 저장)
        await pdf_orchestrator.process_pdf(pdf_request)
        
        logger.info(f"PDF 업로드 및 처리 완료: {upload_request.document_id}")
        
        return JSONResponse(
            status_code=200,
            content={
                "document_id": upload_request.document_id,
                "filename": file.filename,
                "status": "completed",
                "message": "파일이 업로드되고 처리가 완료되었습니다."
            }
        )
        
    except Exception as e:
        logger.error(f"PDF 업로드 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/processing-status/{request_id}")
async def get_processing_status(request_id: str):
    """처리 상태 조회"""
    try:
        # MongoDB에서 처리 상태 조회
        processing_request = await app_state['db'].processing_requests.find_one(
            {"_id": ObjectId(request_id)}
        )
        
        if not processing_request:
            raise HTTPException(status_code=404, detail="처리 요청을 찾을 수 없습니다.")
        
        # 청크 및 임베딩 수 조회
        chunks_count = await app_state['db'].pdf_chunks.count_documents(
            {"processing_request_id": request_id}
        )
        
        # Qdrant에서 임베딩 수 조회
        try:
            collection_name = "pdf_embeddings"
            # 컬렉션이 존재하는지 확인
            collections = app_state['qdrant_client'].get_collections()
            if any(col.name == collection_name for col in collections.collections):
                # 포인트 수 조회
                collection_info = app_state['qdrant_client'].get_collection(collection_name)
                embeddings_count = collection_info.points_count
            else:
                embeddings_count = 0
        except Exception as e:
            logger.error(f"Qdrant 조회 오류: {e}")
            embeddings_count = 0
        
        status = ProcessingStatus(
            id=request_id,
            status=processing_request['status'],
            progress=processing_request.get('progress', 0),
            message=processing_request.get('message'),
            chunks_count=chunks_count,
            embeddings_count=embeddings_count,
            created_at=processing_request['created_at'],
            updated_at=processing_request.get('updated_at')
        )
        
        return status
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"처리 상태 조회 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/processing-requests")
async def list_processing_requests(limit: int = 10, skip: int = 0):
    """처리 요청 목록 조회"""
    try:
        cursor = app_state['db'].processing_requests.find().sort("created_at", -1).skip(skip).limit(limit)
        requests = []
        
        async for doc in cursor:
            requests.append({
                "id": str(doc["_id"]),
                "filename": doc["filename"],
                "status": doc["status"],
                "progress": doc.get("progress", 0),
                "created_at": doc["created_at"].isoformat(),
                "updated_at": doc.get("updated_at", doc["created_at"]).isoformat()
            })
        
        total_count = await app_state['db'].processing_requests.count_documents({})
        
        return {
            "requests": requests,
            "total": total_count,
            "limit": limit,
            "skip": skip
        }
        
    except Exception as e:
        logger.error(f"처리 요청 목록 조회 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/processing-request/{request_id}")
async def delete_processing_request(request_id: str):
    """처리 요청 삭제"""
    try:
        # MongoDB에서 처리 요청 조회
        processing_request = await app_state['db'].processing_requests.find_one(
            {"_id": ObjectId(request_id)}
        )
        
        if not processing_request:
            raise HTTPException(status_code=404, detail="처리 요청을 찾을 수 없습니다.")
        
        # 임시 파일 삭제
        if 'file_path' in processing_request and os.path.exists(processing_request['file_path']):
            os.remove(processing_request['file_path'])
        
        # MongoDB에서 관련 데이터 삭제
        await app_state['db'].processing_requests.delete_one({"_id": ObjectId(request_id)})
        await app_state['db'].pdf_chunks.delete_many({"processing_request_id": request_id})
        
        # Qdrant에서 임베딩 삭제
        try:
            collection_name = "pdf_embeddings"
            collections = app_state['qdrant_client'].get_collections()
            if any(col.name == collection_name for col in collections.collections):
                # 해당 request_id의 포인트들 삭제
                app_state['qdrant_client'].delete(
                    collection_name=collection_name,
                    points_selector={
                        "filter": {
                            "must": [
                                {
                                    "key": "processing_request_id",
                                    "match": {"value": request_id}
                                }
                            ]
                        }
                    }
                )
        except Exception as e:
            logger.error(f"Qdrant에서 임베딩 삭제 중 오류: {e}")
        
        return {"message": "처리 요청이 삭제되었습니다."}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"처리 요청 삭제 중 오류 발생: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    
    # .env 파일 로드
    from dotenv import load_dotenv
    env_file = '.env.development' if os.path.exists('.env.development') else '.env'
    load_dotenv(env_file)
    
    # 서버 실행
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

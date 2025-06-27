# infra/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """전역 설정 관리"""
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    # MongoDB (환경변수에서만 가져옴)
    MONGODB_URL: str
    MONGODB_DATABASE: str
    
    # Qdrant (연결정보는 환경변수에서만)
    QDRANT_URL: str
    QDRANT_COLLECTION_NAME: str = "documents"
    QDRANT_VECTOR_SIZE: int = 1536
    QDRANT_API_KEY: Optional[str] = None
    
    # Kafka (기본값 제공)
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_ID: str = "iacsrag-dev"
    
    # Kafka Topics (기본값 제공)
    KAFKA_TOPIC_DOCUMENT_UPLOADED: str = "document.uploaded"
    KAFKA_TOPIC_TEXT_EXTRACTED: str = "text.extracted"
    KAFKA_TOPIC_CHUNKS_CREATED: str = "chunks.created"
    KAFKA_TOPIC_EMBEDDINGS_GENERATED: str = "embeddings.generated"
    KAFKA_TOPIC_EMAIL_RECEIVED: str = "email.received"
    
    # OpenAI (민감한 정보는 환경변수에서만)
    OPENAI_API_KEY: str
    OPENAI_BASE_URL: str
    OPENAI_EMBEDDING_MODEL: str
    
    # File Upload
    TEMP_UPLOAD_DIR: str = "./temp/uploads"
    MAX_UPLOAD_SIZE: int = 52428800  # 50MB
    
    # PDF Processing
    PDF_BATCH_SIZE: int = 50  # 배치당 청크 수
    PDF_CHUNK_SIZE: int = 1000
    PDF_CHUNK_OVERLAP: int = 200
    PDF_MAX_CONCURRENT_PROCESSING: int = 3  # 동시 처리 PDF 개수 추가
    
    # Logging
    LOG_LEVEL: str = "DEBUG"
    
    # Processing Logging
    PROCESSING_LOG_MODE: str = "console"  # file|console|both|none
    PROCESSING_LOG_FILE: str = "logs/processing.log"
    PROCESSING_LOG_LEVEL: str = "INFO"  # DEBUG|INFO|WARNING|ERROR
    PROCESSING_LOG_ROTATION: bool = True
    PROCESSING_LOG_MAX_SIZE: str = "10MB"
    PROCESSING_LOG_BACKUP_COUNT: int = 5
    
    class Config:
        env_file = ".env.development"
        case_sensitive = True
        extra = "ignore"  # 추가 환경 변수 무시

settings = Settings()

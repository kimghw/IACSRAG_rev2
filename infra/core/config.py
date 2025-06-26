# infra/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    """전역 설정 관리"""
    
    # API Settings
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    
    # MongoDB
    MONGODB_URL: str
    MONGODB_DATABASE: str
    
    # Qdrant
    QDRANT_URL: str
    QDRANT_COLLECTION_NAME: str = "documents"
    QDRANT_VECTOR_SIZE: int = 1536
    QDRANT_API_KEY: Optional[str] = None
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = "localhost:9092"
    KAFKA_CONSUMER_GROUP_ID: str = "iacsrag-dev"
    
    # Kafka Topics
    KAFKA_TOPIC_DOCUMENT_UPLOADED: str = "document.uploaded"
    KAFKA_TOPIC_TEXT_EXTRACTED: str = "text.extracted"
    KAFKA_TOPIC_CHUNKS_CREATED: str = "chunks.created"
    KAFKA_TOPIC_EMBEDDINGS_GENERATED: str = "embeddings.generated"
    KAFKA_TOPIC_EMAIL_RECEIVED: str = "email.received"
    
    # OpenRouter
    OPENROUTER_API_KEY: str
    OPENROUTER_BASE_URL: str = "https://openrouter.ai/api/v1"
    OPENROUTER_EMBEDDING_MODEL: str = "text-embedding-3-small"
    
    # File Upload
    TEMP_UPLOAD_DIR: str = "./temp/uploads"
    MAX_UPLOAD_SIZE: int = 52428800  # 50MB
    
    # PDF Processing
    PDF_BATCH_SIZE: int = 10  # 배치당 청크 수
    PDF_CHUNK_SIZE: int = 1000
    PDF_CHUNK_OVERLAP: int = 200
    
    # Logging
    LOG_LEVEL: str = "DEBUG"
    
    class Config:
        env_file = ".env.development"
        case_sensitive = True

settings = Settings()
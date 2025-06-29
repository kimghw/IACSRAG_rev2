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

    # Upload 설정
    UPLOAD_APPLY_HASHER: bool = False  # 파일 중복 검사 활성화
    UPLOAD_MAX_CONCURRENT: int = 10  # 동시 업로드 처리 수 제한
    
    # ===== PDF Processing - 청킹 전략 선택 =====
    PDF_CHUNKING_STRATEGY: str = "character"  # character|token|semantic|recursive_semantic|spacy_semantic|embedding_semantic
    
    # ===== Character 청킹 전략 설정 =====
    PDF_CHAR_CHUNK_SIZE: int = 1200  # 청크당 최대 문자 수
    PDF_CHAR_CHUNK_OVERLAP: int = 300  # 청크 간 오버랩 문자 수
    
    # ===== Token 청킹 전략 설정 =====
    PDF_TOKEN_MODEL: str = "gpt-3.5-turbo"  # 토큰 계산용 모델
    PDF_TOKEN_MAX_TOKENS: int = 500  # 청크당 최대 토큰 수
    PDF_TOKEN_OVERLAP: int = 50  # 토큰 단위 오버랩
    
    # ===== Semantic 청킹 전략 설정 (semantic, recursive_semantic) =====
    PDF_SEMANTIC_MAX_CHUNK_SIZE: int = 1500  # 최대 청크 크기 (문자)
    PDF_SEMANTIC_MIN_CHUNK_SIZE: int = 400   # 최소 청크 크기 (문자)
    PDF_SEMANTIC_CHUNK_OVERLAP: int = 300    # 청크 간 오버랩 (문자)
    
    # ===== SpaCy Semantic 청킹 전략 설정 =====
    PDF_SPACY_MAX_CHUNK_SIZE: int = 2000     # 최대 청크 크기 (문자)
    PDF_SPACY_MIN_CHUNK_SIZE: int = 500      # 최소 청크 크기 (문자)
    PDF_SPACY_TOPIC_THRESHOLD: float = 0.5   # 주제 유사도 임계값 (0.0~1.0)
    PDF_SPACY_LANGUAGE_MODEL: str = "en_core_web_sm"  # SpaCy 언어 모델
    
    # ===== Embedding Semantic 청킹 전략 설정 =====
    PDF_EMBED_MAX_CHUNK_SIZE: int = 1500     # 최대 청크 크기 (문자)
    PDF_EMBED_MIN_CHUNK_SIZE: int = 400      # 최소 청크 크기 (문자)  
    PDF_EMBED_SIMILARITY_THRESHOLD: float = 0.8  # 임베딩 유사도 임계값
    PDF_EMBED_CACHE_EMBEDDINGS: bool = True      # 임베딩 캐싱 여부
    
    # ===== 기존 설정 (하위 호환성) =====
    # Deprecated: 새 코드에서는 전략별 설정 사용 권장
    PDF_CHUNK_SIZE: int = 1000  
    PDF_CHUNK_OVERLAP: int = 200
    PDF_MAX_TOKENS_PER_CHUNK: int = 500
    PDF_SEMANTIC_SIMILARITY_THRESHOLD: float = 0.8
    PDF_SEMANTIC_MODEL: str = "text-embedding-ada-002"
    
    # ===== PDF Processing - 배치 및 동시성 =====
    PDF_BATCH_SIZE: int = 300  # 메인 배치 크기 (청크 배치)
    PDF_SUB_BATCH_SIZE: int = 300  # OpenAI API 서브 배치 크기
    PDF_MAX_CONCURRENT_PROCESSING: int = 5  # 동시 처리 PDF 개수
    PDF_MAX_CONCURRENT_API_CALLS: int = 7  # 동시 OpenAI API 호출 수
    PDF_MAX_CONCURRENT_BATCHES: int = 5  # 동시 처리 배치 수
    
    # ===== Logging 설정 =====
    LOG_LEVEL: str = "INFO"
    
    # Processing Logging
    PROCESSING_LOG_MODE: str = "console"  # file|console|both|none
    PROCESSING_LOG_FILE: str = "logs/processing.log"
    PROCESSING_LOG_LEVEL: str = "INFO"  # DEBUG|INFO|WARNING|ERROR
    PROCESSING_LOG_ROTATION: bool = True
    PROCESSING_LOG_MAX_SIZE: str = "10MB"
    PROCESSING_LOG_BACKUP_COUNT: int = 5
    
    # PDF 임베딩 재시도 설정
    PDF_EMBEDDING_MAX_RETRIES: int = 3
    PDF_EMBEDDING_RETRY_DELAY: float = 1.0

    class Config:
        env_file = ".env.development"
        case_sensitive = True
        extra = "ignore"  # 추가 환경 변수 무시

settings = Settings()
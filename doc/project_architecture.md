# 콘텐츠 처리 시스템 아키텍처

## 1. 인프라스트럭처 (infra/)

### 1.1 infra/core/
```
infra/
├── core/
│   ├── __init__.py
│   ├── config.py          # 전역 설정 관리 (.env 로드)
│   ├── connections.py     # DB 연결 정보
│   └── docker_manager.py  # 컨테이너 관리
```

### 1.2 infra/databases/
```
├── databases/
│   ├── __init__.py
│   ├── qdrant_db.py       # Qdrant 벡터DB 레이지 싱글톤
│   ├── mongo_db.py        # MongoDB 레이지 싱글톤
│   ├── redis_cache.py     # Redis 캐시 레이지 싱글톤
│   └── db_adapter.py      # DB 어댑터 패턴
```

### 1.3 infra/events/
```
├── events/
│   ├── __init__.py
│   ├── kafka_server.py    # Kafka 서버 초기화
│   ├── event_consumer.py  # Kafka 컨슈머 베이스
│   ├── event_producer.py  # Kafka 프로듀서 베이스
│   ├── event_router.py    # 이벤트 라우팅 로직
│   └── topic_manager.py   # 토픽 관리 및 설정
```

### 1.4 infra/api/
```
├── api/
│   ├── __init__.py
│   ├── app.py            # FastAPI 앱 초기화
│   ├── upload_router.py   # 파일 업로드 라우터
│   └── middleware.py      # API 미들웨어
```

## 2. 모듈 구조

### 2.1 upload_service/ (파일 업로드 모듈)
```
upload_service/
├── __init__.py
├── orchestrator.py         # 업로드 처리 플로우
├── schema.py              # 업로드 데이터 스키마
├── upload_handler.py      # 파일 업로드 핸들러
├── upload_validator.py    # 파일 검증 서비스
├── event_publisher.py     # 이벤트 발행 서비스
├── repository.py          # 업로드 이력 저장
└── readme.md
```

**데이터 플로우:**
1. API로 파일 업로드 수신
2. 파일 검증 (크기, 타입)
3. 임시 저장 및 메타데이터 생성
4. 이벤트 발행 (document.uploaded)
5. 업로드 이력 MongoDB 저장

### 2.2 content_markdown/ (마크다운 처리 모듈)
```
content_markdown/
├── __init__.py
├── orchestrator.py        # 마크다운 처리 플로우
├── schema.py             # 입출력 데이터 스키마
├── markdown_processor.py  # 마크다운 처리 서비스
├── markdown_chunker.py    # 마크다운 청킹 서비스
├── markdown_embedder.py   # 임베딩 서비스
├── markdown_consumer.py   # Kafka 컨슈머
├── repository.py         # DB 접근 계층
└── readme.md
```

**데이터 플로우:**
1. 이벤트 구독 (document.uploaded - markdown 타입)
2. 마크다운 처리 (사용자 요구사항 있으면 처리, 없으면 원본)
3. 청킹 (마크다운 특화)
4. 임베딩
5. Qdrant 벡터DB 저장

### 2.3 content_json/ (JSON 처리 모듈)
```
content_json/
├── __init__.py
├── orchestrator.py        # JSON 처리 플로우
├── schema.py             # 입출력 데이터 스키마
├── json_processor.py      # JSON 파싱 서비스
├── json_key_embedder.py   # 특정 키 임베딩 서비스
├── json_consumer.py       # Kafka 컨슈머
├── repository.py         # DB 접근 계층
└── readme.md
```

**데이터 플로우:**
1. 이벤트 구독 (document.uploaded - json 타입)
2. 원본 MongoDB 저장
3. JSON 파싱
4. 2-3개 키의 값 임베딩
5. Qdrant 벡터DB 저장

### 2.4 content_pdf/ (PDF 처리 모듈)
```
content_pdf/
├── __init__.py
├── orchestrator.py        # PDF 처리 플로우
├── schema.py             # 입출력 데이터 스키마
├── pdf_processor.py       # PDF 처리 서비스
├── pdf_chunker.py         # 의미론적 청킹 서비스
├── pdf_embedder.py        # 임베딩 서비스
├── pdf_consumer.py        # Kafka 컨슈머
├── event_publisher.py     # 처리 완료 이벤트 발행
├── repository.py         # DB 접근 계층
└── readme.md
```

**데이터 플로우:**
1. 이벤트 구독 (document.uploaded - pdf 타입)
2. PDF 처리 및 텍스트 추출
3. 이벤트 발행 (text.extracted)
4. 의미론적 청킹
5. 이벤트 발행 (chunks.created)
6. 임베딩
7. 이벤트 발행 (embeddings.generated)
8. Qdrant 벡터DB 저장

### 2.5 content_email/ (이메일 처리 모듈)
```
content_email/
├── __init__.py
├── orchestrator.py        # 이메일 처리 플로우
├── schema.py             # 입출력 데이터 스키마
├── email_processor.py     # 이메일 파싱 서비스
├── email_embedder.py      # 1-2개 필드 임베딩 서비스
├── email_consumer.py      # Kafka 컨슈머
├── repository.py         # DB 접근 계층
└── readme.md
```

**데이터 플로우:**
1. 이벤트 구독 (email.graph_iacs)
2. JSON 파싱 (사용자 데이터 스키마)
3. 청킹 없음
4. 1-2개 필드 임베딩
5. Qdrant 벡터DB 저장

## 3. 루트 스키마 (schema.py)
```python
# 모듈 간 공통 데이터 계약
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
from datetime import datetime

class ContentEvent(BaseModel):
    """이벤트 기본 스키마"""
    event_type: str  # event_content_pdf, event_content_md, etc.
    content_id: str
    content_data: Dict[str, Any]
    metadata: Optional[Dict[str, Any]]
    timestamp: datetime

class ProcessedContent(BaseModel):
    """처리된 콘텐츠 기본 스키마"""
    content_id: str
    original_type: str
    processed_data: Any
    embeddings: Optional[List[Dict[str, Any]]]
    metadata: Dict[str, Any]
    processed_at: datetime

class ChunkData(BaseModel):
    """청크 데이터 스키마"""
    chunk_id: str
    content_id: str
    chunk_text: str
    chunk_index: int
    metadata: Optional[Dict[str, Any]]

class EmbeddingData(BaseModel):
    """임베딩 데이터 스키마"""
    embedding_id: str
    content_id: str
    chunk_id: Optional[str]
    embedding_vector: List[float]
    embedding_text: str
    metadata: Dict[str, Any]
```

## 4. 구현 우선순위

1. **Phase 1: 인프라 구축**
   - 이벤트 서버 설정
   - DB 연결 설정 (MongoDB, VectorDB)
   - 기본 이벤트 라우팅

2. **Phase 2: 모듈 구현**
   - 각 모듈의 orchestrator 구현
   - 모듈별 프로세서 구현
   - repository 패턴 구현

3. **Phase 3: 통합 및 테스트**
   - 모듈 간 통합 테스트
   - 성능 모니터링 설정
   - OpenTelemetry Trace 설정

## 5. 주의사항

- 모듈 간 순환 참조 방지
- 각 서비스는 의존성 없이 생성, Orchestrator가 의존성 주입
- 공통 기능은 2-3곳 이상 중복 시 infra로 승급
- 과도한 추상화 지양 (YAGNI 원칙)

## 6. 네이밍 규칙

### 6.1 파일명
- **모듈 내부 파일**: `{module_name}_{function}.py`
  - 예: `pdf_processor.py`, `pdf_chunker.py`
- **컨슈머 파일**: `{module_name}_consumer.py`
- **이벤트 발행**: `event_publisher.py` (모듈 내 통일)
- **공통 파일**: `orchestrator.py`, `schema.py`, `repository.py`

### 6.2 클래스명
- **프로세서**: `{ModuleName}Processor`
  - 예: `PdfProcessor`, `MarkdownProcessor`
- **컨슈머**: `{ModuleName}Consumer`
- **발행자**: `EventPublisher` (모듈별 커스터마이징)
- **오케스트레이터**: `{ModuleName}Orchestrator`

### 6.3 함수명
- **공개 함수**: `{action}_{target}`
  - 예: `process_pdf()`, `chunk_markdown()`
- **내부 함수**: `_{action}_{target}`
  - 예: `_validate_format()`, `_prepare_data()`
- **이벤트 핸들러**: `handle_{event_type}`
  - 예: `handle_document_uploaded()`

### 6.4 이벤트/토픽명
- **형식**: `{entity}.{action}`
- **예시**:
  - `document.uploaded`
  - `text.extracted`
  - `chunks.created`
  - `embeddings.generated`
  - `email.graph_iacs`

### 6.5 환경변수명
- **형식**: `{SERVICE}_{PROPERTY}`
- **예시**:
  - `MONGODB_URL`
  - `QDRANT_COLLECTION_NAME`
  - `KAFKA_TOPIC_DOCUMENT_UPLOADED`

## 7. 외부 시스템 연동

### 7.1 Docker 컨테이너 구성
- **MongoDB** (포트: 27017): 메인 데이터베이스
- **Qdrant** (포트: 6333, 6334): 벡터 데이터베이스
- **Kafka** (포트: 9092): 메시지 브로커
- **Zookeeper**: Kafka 메타데이터 관리
- **Redis** (포트: 6379): 캐시 (선택)
- **Kafka-UI** (포트: 8080): 개발용 모니터링

### 7.2 환경 설정 (.env.development)
```
# MongoDB
MONGODB_URL=mongodb://admin:password@localhost:27017
MONGODB_DATABASE=iacsrag_dev

# Qdrant
QDRANT_URL=http://localhost:6333
QDRANT_COLLECTION_NAME=documents
QDRANT_VECTOR_SIZE=1536

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CONSUMER_GROUP_ID=iacsrag-dev

# Kafka Topics
KAFKA_TOPIC_DOCUMENT_UPLOADED=document.uploaded
KAFKA_TOPIC_TEXT_EXTRACTED=text.extracted
KAFKA_TOPIC_CHUNKS_CREATED=chunks.created
KAFKA_TOPIC_EMBEDDINGS_GENERATED=embeddings.generated
KAFKA_TOPIC_EMAIL_GRAPH_IACS=email.graph_iacs
```
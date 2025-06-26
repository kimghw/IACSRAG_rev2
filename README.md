```markdown
# IACSRAG_rev2

이벤트 기반 콘텐츠 처리 시스템

## 시스템 구성

- **MongoDB**: 문서 및 메타데이터 저장
- **Qdrant**: 벡터 임베딩 저장
- **Kafka**: 이벤트 기반 메시지 처리
- **FastAPI**: REST API 서버

## 요구사항

- Python 3.8+
- Docker & Docker Compose
- 8GB+ RAM (권장)

## 빠른 시작

### 1. 환경 설정
`.env.development` 파일을 생성하고 필요한 환경변수를 설정합니다.

### 2. 의존성 설치
```bash
pip install -r requirements.txt
```

### 3. 시스템 초기화
```bash
# Docker와 모든 서비스 자동 초기화
python main.py init
```

초기화 과정:
- Docker 컨테이너 상태 확인 및 시작
- MongoDB 컬렉션 및 인덱스 생성
- Qdrant 벡터 컬렉션 생성
- Kafka 토픽 생성

### 4. 서버 실행
```bash
# API 서버 시작
python main.py
```

## API 엔드포인트

- `POST /api/v1/upload/pdf` - PDF 파일 업로드
- `GET /health` - 헬스체크

### PDF 업로드 예시
```bash
curl -X POST "http://localhost:8000/api/v1/upload/pdf" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@document.pdf" \
  -F 'metadata={"author":"John Doe","category":"research"}'
```

## 서비스 URL

- API Server: http://localhost:8000
- API Docs: http://localhost:8000/docs
- Kafka UI: http://localhost:8080
- Qdrant Dashboard: http://localhost:6333/dashboard

## 처리 흐름

1. PDF 업로드 → MongoDB GridFS 저장
2. Kafka 이벤트 발행 (`document.uploaded`)
3. PDF Consumer가 이벤트 수신 및 처리
4. 텍스트 추출 → 청킹 → 임베딩 생성
5. Qdrant에 벡터 저장

## 주요 설정

### 환경변수 (`.env.development`)
- `PDF_MAX_CONCURRENT_PROCESSING`: 동시 처리 PDF 개수 (기본: 3)
- `PDF_BATCH_SIZE`: 임베딩 배치 크기 (기본: 10)
- `PDF_CHUNK_SIZE`: 청크 크기 (기본: 1000)

## 문제 해결

### 서비스 초기화 실패 시
```bash
# 개별 서비스 상태 확인
docker-compose ps

# 로그 확인
docker-compose logs [service-name]

# 수동 초기화
docker-compose up -d
```

### 포트 충돌 시
- MongoDB: 27017
- Kafka: 9092
- Qdrant: 6333, 6334
- API Server: 8000

해당 포트가 사용 중인지 확인하고 필요시 `docker-compose.yml`에서 포트 변경

## 개발 가이드

### 로깅 레벨 변경
`.env.development`에서 `LOG_LEVEL` 조정:
- DEBUG: 상세 로그
- INFO: 일반 정보
- WARNING: 경고
- ERROR: 에러만

### 새 컨슈머 추가
1. `content_[type]/` 폴더 생성
2. `[type]_consumer.py` 구현
3. `main.py`에 컨슈머 등록
```
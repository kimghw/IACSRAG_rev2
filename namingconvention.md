

## 1.  네이밍 규칙 가이드 (업데이트)

### 2.1 파일명
```
모듈 내부: {module_name}_{function}.py
- pdf_processor.py (O)
- pdf_chunker.py (O)
- processor.py (X) - 모듈명 누락

컨슈머: {module_name}_consumer.py
- pdf_consumer.py (O)
- markdown_consumer.py (O)

공통 파일:
- orchestrator.py (모든 모듈 통일)
- repository.py (모든 모듈 통일)
- schema.py (루트 및 각 모듈)
```

### 2.2 클래스명
```
프로세서: {ModuleName}Processor
- PdfProcessor (O)
- MarkdownProcessor (O)
- Processor (X) - 모듈명 누락

컨슈머: {ModuleName}Consumer
- PdfConsumer (O)
- EmailConsumer (O)

오케스트레이터: {ModuleName}Orchestrator
- PdfOrchestrator (O)
- UploadOrchestrator (O)

레포지토리: {ModuleName}Repository
- PdfRepository (O)
- JsonRepository (O)
```

### 2.3 함수명
```
공개 함수: {action}_{target}()
- process_pdf() (O)
- extract_text() (O)
- create_chunks() (O)
- generate_embeddings() (O)

내부 함수: _{action}_{target}()
- _validate_format() (O)
- _get_embeddings() (O)
- _ensure_producer() (O)

이벤트 핸들러: handle_{event_type}()
- handle_message() (O)
- handle_document_uploaded() (O)
```

### 2.4 스키마명 (Pydantic 모델)
```
요청/응답: {ModuleName}{Purpose}
- PdfProcessingRequest (O)
- UploadRequest (O)
- UploadResponse (O)

데이터 모델: {ModuleName}{DataType}
- PdfChunkData (O)
- PdfEmbeddingData (O)
- MarkdownChunkData (O)

이벤트: {ModuleName}{EventName}Event
- UploadDocumentEvent (O)
- PdfTextExtractedEvent (O)
```

### 2.5 이벤트/토픽명
```
형식: {entity}.{action}
- document.uploaded (O)
- text.extracted (O)
- chunks.created (O)
- embeddings.generated (O)
- email.received (O)
```

### 2.6 환경변수명
```
형식: {SERVICE}_{PROPERTY}
- MONGODB_URL (O)
- MONGODB_DATABASE (O)
- QDRANT_COLLECTION_NAME (O)
- KAFKA_TOPIC_DOCUMENT_UPLOADED (O)
- OPENROUTER_API_KEY (O)
- OPENROUTER_EMBEDDING_MODEL (O)
```

### 2.7 컬렉션/테이블명
```
MongoDB 컬렉션: 복수형, 소문자, 언더스코어
- uploads (O)
- processed_documents (O)
- email_contents (O)

Qdrant 컬렉션: 복수형, 소문자
- documents (O)
- embeddings (O)
```

이 네이밍 규칙을 따르면 코드의 일관성과 가독성이 크게 향상됩니다!
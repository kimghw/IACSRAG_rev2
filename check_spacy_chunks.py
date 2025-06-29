#!/usr/bin/env python3
"""
SpaCy 청킹 결과 확인 스크립트
MongoDB에서 최근 업로드된 문서의 청킹 결과를 분석합니다.
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import statistics

# MongoDB 연결 설정
MONGODB_URL = 'mongodb://admin:password@localhost:27017'
MONGODB_DATABASE = 'iacsrag_dev'

async def analyze_spacy_chunks():
    """SpaCy로 처리된 청크들을 분석"""
    client = AsyncIOMotorClient(MONGODB_URL)
    db = client[MONGODB_DATABASE]
    
    print("🔍 MongoDB 연결 성공")
    print("="*80)
    
    # 가장 최근 업로드된 문서 찾기
    latest_doc = await db.uploads.find_one(
        {},
        sort=[("uploaded_at", -1)]
    )
    
    if not latest_doc:
        print("❌ 업로드된 문서가 없습니다.")
        return
    
    document_id = latest_doc['document_id']
    print(f"📄 분석할 문서 정보:")
    print(f"   - Document ID: {document_id}")
    print(f"   - 파일명: {latest_doc['filename']}")
    print(f"   - 업로드 시간: {latest_doc['uploaded_at']}")
    print(f"   - 상태: {latest_doc.get('status', 'N/A')}")
    print("\n" + "="*80 + "\n")
    
    # 해당 문서의 모든 청크 가져오기
    total_chunks = await db.pdf_chunks.count_documents({'document_id': document_id})
    print(f"📊 총 청크 수: {total_chunks}")
    
    if total_chunks == 0:
        print("❌ 청크가 없습니다.")
        return
    
    # 처음 5개 청크 상세 정보
    print(f"\n📝 처음 5개 청크 상세 정보:")
    print("-"*80)
    
    chunks_sample = await db.pdf_chunks.find(
        {"document_id": document_id},
        sort=[("chunk_index", 1)]
    ).to_list(length=5)
    
    for chunk in chunks_sample:
        print(f"\n🔸 청크 #{chunk['chunk_index']}")
        print(f"   ID: {chunk['_id']}")
        print(f"   크기: {chunk['char_end'] - chunk['char_start']} 문자")
        
        # 텍스트 미리보기
        text_preview = chunk['text_content'][:150].replace('\n', ' ')
        print(f"   텍스트: {text_preview}...")
        
        # 메타데이터 확인
        metadata = chunk.get('chunk_metadata', {})
        print(f"\n   📋 메타데이터:")
        print(f"      - 청킹 전략: {metadata.get('chunking_strategy', 'N/A')}")
        print(f"      - 청크 크기: {metadata.get('chunk_size', 'N/A')}")
        
        # SpaCy 특유의 정보 (entities, topics)
        entities = metadata.get('entities', [])
        topics = metadata.get('topics', [])
        
        print(f"      - 엔티티: {len(entities)}개")
        if entities:
            print(f"        → {', '.join(entities[:5])}{'...' if len(entities) > 5 else ''}")
        
        print(f"      - 토픽: {len(topics)}개")
        if topics:
            print(f"        → {', '.join(topics[:10])}{'...' if len(topics) > 10 else ''}")
        
        # 임베딩 상태
        embedding_status = chunk.get('embedding_status', 'N/A')
        embedding_id = chunk.get('embedding_id', None)
        print(f"      - 임베딩 상태: {embedding_status}")
        if embedding_id:
            print(f"      - 임베딩 ID: {embedding_id[:16]}...")
    
    print("\n" + "="*80 + "\n")
    
    # 전체 통계 분석
    print("📊 전체 청킹 통계:")
    print("-"*80)
    
    # 모든 청크 가져와서 분석
    all_chunks = await db.pdf_chunks.find(
        {"document_id": document_id}
    ).to_list(length=None)
    
    # 청크 크기 분석
    chunk_sizes = [chunk['char_end'] - chunk['char_start'] for chunk in all_chunks]
    avg_size = sum(chunk_sizes) / len(chunk_sizes)
    median_size = statistics.median(chunk_sizes)
    
    print(f"   📏 크기 분석:")
    print(f"      - 평균 크기: {avg_size:.0f} 문자")
    print(f"      - 중간값: {median_size:.0f} 문자")
    print(f"      - 최소 크기: {min(chunk_sizes)} 문자")
    print(f"      - 최대 크기: {max(chunk_sizes)} 문자")
    print(f"      - 표준편차: {statistics.stdev(chunk_sizes):.0f} 문자")
    
    # 엔티티/토픽 분석
    total_entities = 0
    total_topics = 0
    chunks_with_entities = 0
    chunks_with_topics = 0
    all_entities = set()
    all_topics = set()
    
    for chunk in all_chunks:
        metadata = chunk.get('chunk_metadata', {})
        entities = metadata.get('entities', [])
        topics = metadata.get('topics', [])
        
        if entities:
            chunks_with_entities += 1
            total_entities += len(entities)
            all_entities.update(entities)
        
        if topics:
            chunks_with_topics += 1
            total_topics += len(topics)
            all_topics.update(topics)
    
    print(f"\n   🏷️ 엔티티 분석:")
    print(f"      - 엔티티가 있는 청크: {chunks_with_entities}/{len(all_chunks)} ({chunks_with_entities/len(all_chunks)*100:.1f}%)")
    print(f"      - 총 엔티티 수: {total_entities}")
    print(f"      - 고유 엔티티 수: {len(all_entities)}")
    if chunks_with_entities > 0:
        print(f"      - 청크당 평균 엔티티: {total_entities/chunks_with_entities:.1f}개")
    
    print(f"\n   📚 토픽 분석:")
    print(f"      - 토픽이 있는 청크: {chunks_with_topics}/{len(all_chunks)} ({chunks_with_topics/len(all_chunks)*100:.1f}%)")
    print(f"      - 총 토픽 수: {total_topics}")
    print(f"      - 고유 토픽 수: {len(all_topics)}")
    if chunks_with_topics > 0:
        print(f"      - 청크당 평균 토픽: {total_topics/chunks_with_topics:.1f}개")
    
    # 임베딩 상태 확인
    print(f"\n   🧠 임베딩 상태:")
    embedding_statuses = {}
    for chunk in all_chunks:
        status = chunk.get('embedding_status', 'unknown')
        embedding_statuses[status] = embedding_statuses.get(status, 0) + 1
    
    for status, count in embedding_statuses.items():
        print(f"      - {status}: {count}개 ({count/len(all_chunks)*100:.1f}%)")
    
    # 상위 엔티티/토픽 표시
    if all_entities:
        print(f"\n   🌟 상위 엔티티 (최대 20개):")
        entity_counts = {}
        for chunk in all_chunks:
            for entity in chunk.get('chunk_metadata', {}).get('entities', []):
                entity_counts[entity] = entity_counts.get(entity, 0) + 1
        
        sorted_entities = sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        for entity, count in sorted_entities:
            print(f"      - {entity}: {count}회")
    
    if all_topics:
        print(f"\n   📖 상위 토픽 (최대 20개):")
        topic_counts = {}
        for chunk in all_chunks:
            for topic in chunk.get('chunk_metadata', {}).get('topics', []):
                topic_counts[topic] = topic_counts.get(topic, 0) + 1
        
        sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)[:20]
        for topic, count in sorted_topics:
            print(f"      - {topic}: {count}회")
    
    print("\n" + "="*80)
    print("✅ 분석 완료!")
    
    client.close()

# 스크립트 실행
if __name__ == "__main__":
    asyncio.run(analyze_spacy_chunks())
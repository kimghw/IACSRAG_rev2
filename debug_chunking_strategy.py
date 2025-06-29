#!/usr/bin/env python3
"""
실제 사용되는 청킹 전략을 확인하는 스크립트
"""
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from infra.core.config import settings
from content_pdf.services.chunking_service import ChunkingService

async def check_chunking_details(document_id: str):
    """청킹 상세 정보 확인"""
    
    # 1. 환경 설정 확인
    print(f"\n=== 환경 설정 ===")
    print(f"PDF_CHUNKING_STRATEGY: {settings.PDF_CHUNKING_STRATEGY}")
    print(f"PDF_CHUNK_SIZE: {settings.PDF_CHUNK_SIZE}")
    print(f"PDF_CHUNK_OVERLAP: {settings.PDF_CHUNK_OVERLAP}")
    
    # 2. ChunkingService 생성 시 실제 전략 확인
    print(f"\n=== ChunkingService 초기화 ===")
    service = ChunkingService()
    print(f"사용된 전략: {service.strategy_name}")
    print(f"전략 클래스: {service.strategy.__class__.__name__}")
    print(f"전략 정보: {service.get_strategy_info()}")
    
    # 3. MongoDB에서 청크 메타데이터 확인
    print(f"\n=== 청크 메타데이터 확인 ===")
    client = AsyncIOMotorClient(settings.MONGODB_URL)
    db = client[settings.MONGODB_DATABASE]
    
    # 샘플 청크의 메타데이터 확인
    sample_chunks = await db.pdf_chunks.find(
        {"document_id": document_id},
        {"chunk_metadata": 1, "text_content": 1}
    ).limit(3).to_list(None)
    
    for i, chunk in enumerate(sample_chunks):
        print(f"\n청크 {i}:")
        metadata = chunk.get('chunk_metadata', {})
        print(f"  - 청킹 전략: {metadata.get('chunking_strategy', 'N/A')}")
        print(f"  - 설정된 청크 크기: {metadata.get('chunk_size_config', 'N/A')}")
        print(f"  - 실제 청크 크기: {metadata.get('chunk_size', 'N/A')}")
        print(f"  - 오버랩 설정: {metadata.get('overlap_config', 'N/A')}")
        
        # 텍스트 샘플로 전략 추정
        text = chunk.get('text_content', '')[:200]
        if '\\n\\n' in text:
            print(f"  - 문단 구분 발견: 의미 기반 청킹 가능성")
        else:
            print(f"  - 단순 텍스트: 문자 기반 청킹 가능성")
    
    # 4. 청크 경계 분석
    print(f"\n=== 청크 경계 분석 ===")
    consecutive_chunks = await db.pdf_chunks.find(
        {"document_id": document_id}
    ).sort("chunk_index", 1).limit(5).to_list(None)
    
    for i in range(len(consecutive_chunks) - 1):
        curr = consecutive_chunks[i]
        next_chunk = consecutive_chunks[i + 1]
        
        curr_text = curr.get('text_content', '')
        next_text = next_chunk.get('text_content', '')
        
        print(f"\n청크 {i} → {i+1} 경계:")
        print(f"  현재 청크 끝: ...{curr_text[-50:]}")
        print(f"  다음 청크 시작: {next_text[:50]}...")
        
        # 오버랩 확인
        overlap_found = False
        for j in range(min(200, len(curr_text), len(next_text)), 0, -1):
            if curr_text[-j:] == next_text[:j]:
                print(f"  오버랩 발견: {j}자")
                overlap_found = True
                break
        
        if not overlap_found:
            print(f"  오버랩 없음")
    
    client.close()

if __name__ == "__main__":
    # 가장 최근 문서 ID 사용
    document_id = "4915a0e4-dbd4-4a1f-8979-c0749993a714"
    
    import sys
    if len(sys.argv) > 1:
        document_id = sys.argv[1]
    
    asyncio.run(check_chunking_details(document_id))
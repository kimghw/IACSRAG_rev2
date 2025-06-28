# content_pdf/pdf_embedder.py
import asyncio
from typing import List, Dict, Any, Tuple
import httpx
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from datetime import datetime, timezone

from .schema import ChunkDocument
from schema import EmbeddingData
from infra.core.config import settings

logger = logging.getLogger(__name__)

class PdfEmbedder:
    """멀티플렉싱을 지원하는 임베딩 생성 서비스"""
    
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.base_url = settings.OPENAI_BASE_URL
        self.model = settings.OPENAI_EMBEDDING_MODEL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        self.max_concurrent_api_calls = settings.PDF_MAX_CONCURRENT_API_CALLS
        self.api_semaphore = asyncio.Semaphore(self.max_concurrent_api_calls)
        self.sub_batch_size = settings.PDF_SUB_BATCH_SIZE
    
    async def generate_batch_embeddings(
        self, 
        chunks: List[ChunkDocument]
    ) -> Tuple[List[EmbeddingData], Dict[str, str]]:
        """배치 내에서도 멀티플렉싱으로 임베딩 생성"""
        
        # 서브 배치로 분할 (API 제한 고려)
        sub_batches = []
        for i in range(0, len(chunks), self.sub_batch_size):
            sub_batch = chunks[i:i + self.sub_batch_size]
            sub_batches.append(sub_batch)
        
        # 각 서브 배치를 병렬로 처리
        tasks = []
        for sub_batch in sub_batches:
            task = self._process_sub_batch_with_semaphore(sub_batch)
            tasks.append(task)
        
        # 모든 서브 배치를 동시에 처리
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 합치기
        all_embeddings = []
        chunk_to_embedding_map = {}  # chunk_id -> embedding_id
        
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Sub-batch embedding failed: {result}")
                continue
            
            embeddings, chunk_embedding_map = result
            all_embeddings.extend(embeddings)
            chunk_to_embedding_map.update(chunk_embedding_map)
        
        return all_embeddings, chunk_to_embedding_map
    
    async def _process_sub_batch_with_semaphore(
        self, 
        sub_batch: List[ChunkDocument]
    ) -> Tuple[List[EmbeddingData], Dict[str, str]]:
        """세마포어로 API 호출 수 제한"""
        async with self.api_semaphore:
            return await self._process_sub_batch(sub_batch)
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _process_sub_batch(
        self, 
        sub_batch: List[ChunkDocument]
    ) -> Tuple[List[EmbeddingData], Dict[str, str]]:
        """단일 서브 배치 처리"""
        texts = [chunk.text_content for chunk in sub_batch]
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                f"{self.base_url}/embeddings",
                headers=self.headers,
                json={
                    "model": self.model,
                    "input": texts
                }
            )
            
            if response.status_code != 200:
                raise Exception(f"API error: {response.status_code}")
            
            # 응답 처리
            data = response.json()
            vectors = [item['embedding'] for item in data['data']]
            
            # EmbeddingData 생성
            embeddings = []
            chunk_embedding_map = {}  # chunk_id -> embedding_id
            
            for chunk, vector in zip(sub_batch, vectors):
                embedding_id = str(uuid.uuid4())
                
                embedding = EmbeddingData(
                    embedding_id=embedding_id,
                    content_id=chunk.document_id,
                    chunk_id=chunk.chunk_id,
                    embedding_vector=vector,
                    embedding_text=chunk.text_content,
                    metadata=chunk.chunk_metadata
                )
                
                embeddings.append(embedding)
                chunk_embedding_map[chunk.chunk_id] = embedding_id
            
            return embeddings, chunk_embedding_map
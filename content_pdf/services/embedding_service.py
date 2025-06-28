# content_pdf/services/embedding_service.py
import asyncio
import httpx
import uuid
from typing import List, Dict, Any
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from ..schema import ChunkDocument
from schema import EmbeddingData
from infra.core.config import settings

logger = logging.getLogger(__name__)

class EmbeddingService:
    """OpenAI 임베딩 서비스 - 멀티플렉싱 지원"""
    
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.base_url = settings.OPENAI_BASE_URL
        self.model = settings.OPENAI_EMBEDDING_MODEL
        
        # 동시성 제어 - 환경 설정에서 가져오기
        self.max_concurrent = settings.PDF_MAX_CONCURRENT_API_CALLS
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        
        logger.info(f"Embedding service initialized with max concurrent API calls: {self.max_concurrent}")
        
        # 배치 크기
        self.sub_batch_size = settings.PDF_SUB_BATCH_SIZE
        
        # HTTP 클라이언트 설정
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
    async def generate_embeddings_batch(
        self, 
        chunks: List[ChunkDocument]
    ) -> List[EmbeddingData]:
        """배치 임베딩 생성 - 멀티플렉싱"""
        
        if not chunks:
            return []
        
        logger.info(f"Generating embeddings for {len(chunks)} chunks using {self.model}")
        
        # 서브 배치로 분할 (OpenAI API 제한 고려)
        sub_batches = [
            chunks[i:i + self.sub_batch_size] 
            for i in range(0, len(chunks), self.sub_batch_size)
        ]
        
        logger.info(f"Split into {len(sub_batches)} sub-batches of max {self.sub_batch_size} chunks")
        
        # 멀티플렉싱으로 모든 서브 배치 처리
        tasks = [
            self._process_sub_batch(sub_batch, batch_idx) 
            for batch_idx, sub_batch in enumerate(sub_batches)
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 합치기
        all_embeddings = []
        failed_batches = 0
        
        for batch_idx, result in enumerate(results):
            if isinstance(result, Exception):
                failed_batches += 1
                logger.error(f"Sub-batch {batch_idx} failed: {result}")
                continue
            all_embeddings.extend(result)
        
        if failed_batches > 0:
            logger.warning(f"{failed_batches}/{len(sub_batches)} sub-batches failed")
        
        logger.info(f"Generated {len(all_embeddings)} embeddings successfully")
        
        return all_embeddings
    
    async def _process_sub_batch(
        self, 
        chunks: List[ChunkDocument],
        batch_idx: int
    ) -> List[EmbeddingData]:
        """서브 배치 처리 - 세마포어로 동시성 제어"""
        async with self.semaphore:
            logger.debug(f"Processing sub-batch {batch_idx} with {len(chunks)} chunks")
            return await self._call_openai_api(chunks, batch_idx)
    
    @retry(
        stop=stop_after_attempt(3), 
        wait=wait_exponential(multiplier=1, min=4, max=10),
        reraise=True
    )
    async def _call_openai_api(
        self, 
        chunks: List[ChunkDocument],
        batch_idx: int
    ) -> List[EmbeddingData]:
        """OpenAI API 호출"""
        texts = [chunk.text_content for chunk in chunks]
        
        async with httpx.AsyncClient(timeout=30.0) as client:
            try:
                response = await client.post(
                    f"{self.base_url}/embeddings",
                    headers=self.headers,
                    json={
                        "model": self.model,
                        "input": texts
                    }
                )
                
                if response.status_code != 200:
                    error_msg = f"OpenAI API error: {response.status_code} - {response.text}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                # 응답을 EmbeddingData로 변환
                data = response.json()
                embeddings = []
                
                for chunk, embedding_data in zip(chunks, data['data']):
                    embedding = EmbeddingData(
                        embedding_id=str(uuid.uuid4()),
                        content_id=chunk.document_id,
                        chunk_id=chunk.chunk_id,
                        embedding_vector=embedding_data['embedding'],
                        embedding_text=chunk.text_content,
                        metadata={
                            **chunk.chunk_metadata,
                            "model": self.model,
                            "chunk_index": chunk.chunk_index
                        }
                    )
                    embeddings.append(embedding)
                
                logger.debug(f"Sub-batch {batch_idx} completed: {len(embeddings)} embeddings")
                return embeddings
                
            except httpx.TimeoutException:
                logger.error(f"Timeout for sub-batch {batch_idx}")
                raise
            except Exception as e:
                logger.error(f"Error in sub-batch {batch_idx}: {str(e)}")
                raise
# content_pdf/pdf_embedder.py
from typing import List, Dict, Any
import httpx
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential
import logging

from schema import ChunkData, EmbeddingData
from infra.core.config import settings

logger = logging.getLogger(__name__)

class PdfEmbedder:
    """OpenRouter를 사용한 임베딩 생성 서비스"""
    
    def __init__(self):
        self.api_key = settings.OPENROUTER_API_KEY
        self.base_url = settings.OPENROUTER_BASE_URL
        self.model = settings.OPENROUTER_EMBEDDING_MODEL
        self.batch_size = 20
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8000",  # OpenRouter 필수
            "X-Title": "IACSRAG System"  # OpenRouter 권장
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """OpenRouter API를 통한 임베딩 생성"""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{self.base_url}/embeddings",
                headers=self.headers,
                json={
                    "model": self.model,
                    "input": texts
                },
                timeout=30.0
            )
            
            if response.status_code != 200:
                logger.error(f"OpenRouter API error: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            
            data = response.json()
            return [item['embedding'] for item in data['data']]
    
    async def generate_embeddings(self, chunks: List[ChunkData]) -> List[EmbeddingData]:
        """청크 리스트에서 임베딩 생성"""
        embeddings = []
        
        # 배치 처리
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i + self.batch_size]
            texts = [chunk.chunk_text for chunk in batch]
            
            logger.info(f"Generating embeddings for batch {i//self.batch_size + 1}")
            
            # 임베딩 생성
            vectors = await self._get_embeddings(texts)
            
            # EmbeddingData 생성
            for chunk, vector in zip(batch, vectors):
                embedding = EmbeddingData(
                    embedding_id=str(uuid.uuid4()),
                    content_id=chunk.content_id,
                    chunk_id=chunk.chunk_id,
                    embedding_vector=vector,
                    embedding_text=chunk.chunk_text,
                    metadata=chunk.metadata
                )
                embeddings.append(embedding)
        
        logger.info(f"Generated {len(embeddings)} embeddings total")
        return embeddings
# content_pdf/pdf_embedder.py
from typing import List, Dict, Any, Tuple
import httpx
import uuid
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from datetime import datetime, timezone

from .schema import ChunkDocument  # 모듈 내부에서
from schema import EmbeddingData  # 루트에서
from infra.core.config import settings

logger = logging.getLogger(__name__)

class PdfEmbedder:
    """OpenAI를 사용한 임베딩 생성 서비스"""
    
    def __init__(self):
        self.api_key = settings.OPENAI_API_KEY
        self.base_url = settings.OPENAI_BASE_URL
        self.model = settings.OPENAI_EMBEDDING_MODEL
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """OpenAI API를 통한 임베딩 생성"""
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
                logger.error(f"OpenAI API error: {response.text}")
                raise Exception(f"API error: {response.status_code}")
            
            data = response.json()
            return [item['embedding'] for item in data['data']]
    
    async def generate_batch_embeddings(
        self, 
        chunk_batch: List[ChunkDocument]
    ) -> Tuple[List[EmbeddingData], Dict[str, Dict[str, Any]]]:
        """
        배치 단위로 임베딩 생성
        Returns: (embeddings, index_info_map)
        """
        # 텍스트 추출
        texts = [chunk.chunk_data["text"] for chunk in chunk_batch]
        
        logger.info(f"Generating embeddings for batch of {len(texts)} chunks")
        
        # 임베딩 생성
        vectors = await self._get_embeddings(texts)
        
        embeddings = []
        index_info_map = {}  # chunk_id -> index_info
        
        for chunk, vector in zip(chunk_batch, vectors):
            embedding_id = str(uuid.uuid4())
            
            # 인덱싱 정보 생성
            index_info = {
                "embedding_id": embedding_id,
                "vector_size": len(vector),
                "indexed_at": datetime.now(timezone.utc).isoformat()
            }
            
            # EmbeddingData 생성
            embedding = EmbeddingData(
                embedding_id=embedding_id,
                content_id=chunk.document_id,
                chunk_id=chunk.chunk_id,
                embedding_vector=vector,
                embedding_text=chunk.chunk_data["text"],
                metadata={
                    **chunk.chunk_data["metadata"],
                    "chunk_id": chunk.chunk_id,
                    "document_id": chunk.document_id,
                    "chunk_index": chunk.chunk_index
                }
            )
            
            embeddings.append(embedding)
            index_info_map[chunk.chunk_id] = index_info
        
        logger.info(f"Generated {len(embeddings)} embeddings for batch")
        return embeddings, index_info_map

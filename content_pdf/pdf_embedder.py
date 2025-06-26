from typing import List, Dict, Any
import openai
from tenacity import retry, stop_after_attempt, wait_exponential

from schema import ChunkData, EmbeddingData
from infra.core.config import settings

class PdfEmbedder:
    """임베딩 생성 서비스"""
    
    def __init__(self):
        openai.api_key = settings.OPENAI_API_KEY
        self.model = "text-embedding-ada-002"
        self.batch_size = 20
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def _get_embeddings(self, texts: List[str]) -> List[List[float]]:
        """OpenAI API를 통한 임베딩 생성"""
        response = await openai.Embedding.acreate(
            model=self.model,
            input=texts
        )
        return [item['embedding'] for item in response['data']]
    
    async def generate_embeddings(self, chunks: List[ChunkData]) -> List[EmbeddingData]:
        """청크 리스트에서 임베딩 생성"""
        embeddings = []
        
        # 배치 처리
        for i in range(0, len(chunks), self.batch_size):
            batch = chunks[i:i + self.batch_size]
            texts = [chunk.chunk_text for chunk in batch]
            
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
        
        return embeddings
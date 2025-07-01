# content_email/email_embedder.py
import logging
from typing import Dict, Any, List
from openai import AsyncOpenAI
import hashlib
import asyncio

from infra.core.config import settings

logger = logging.getLogger(__name__)

class EmailEmbedder:
    """이메일 임베딩 생성기 - 배치 처리 지원"""
    
    def __init__(self):
        self.client = AsyncOpenAI(
            api_key=settings.OPENAI_API_KEY,
            base_url=settings.OPENAI_BASE_URL
        )
        self.model = settings.OPENAI_EMBEDDING_MODEL
        self.max_concurrent_api_calls = settings.EMAIL_MAX_CONCURRENT_API_CALLS
        self.text_limit = settings.EMAIL_EMBEDDING_TEXT_LIMIT
        self.retry_count = settings.EMAIL_EMBEDDING_RETRY_COUNT
        self.retry_delay = settings.EMAIL_EMBEDDING_RETRY_DELAY
    
    async def generate_batch_embeddings(self, email_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """배치 단위로 임베딩 생성"""
        retry_count = 0
        while retry_count < self.retry_count:
            try:
                logger.info(f"Generating embeddings for batch of {len(email_batch)} emails")
                
                # 텍스트 추출
                texts = [email['text'] for email in email_batch]
                
                # OpenAI API는 한 번에 여러 텍스트 처리 가능
                response = await self.client.embeddings.create(
                    model=self.model,
                    input=texts
                )
                
                # 결과 매핑
                embeddings = []
                for idx, email in enumerate(email_batch):
                    embedding_data = {
                        'embedding_id': self._generate_embedding_id(email['document_id']),
                        'document_id': email['document_id'],
                        'email_id': email['email_id'],
                        'embedding_vector': response.data[idx].embedding,
                        'embedding_text': email['text'][:self.text_limit],
                        'metadata': {
                            **email['metadata'],
                            'embedding_model': self.model,
                            'vector_dimension': len(response.data[idx].embedding)
                        }
                    }
                    embeddings.append(embedding_data)
                
                logger.info(f"Generated {len(embeddings)} embeddings")
                return embeddings
                
            except Exception as e:
                retry_count += 1
                if retry_count < self.retry_count:
                    logger.warning(f"Embedding generation failed, retrying ({retry_count}/{self.retry_count}): {str(e)}")
                    await asyncio.sleep(self.retry_delay)
                else:
                    logger.error(f"Failed to generate batch embeddings after {self.retry_count} retries: {str(e)}")
                    # 실패 시 개별 처리로 폴백
                    return await self._fallback_individual_embeddings(email_batch)
        
        return []
    
    async def _fallback_individual_embeddings(self, email_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """배치 처리 실패 시 개별 처리"""
        logger.warning("Falling back to individual embedding generation")
        
        embeddings = []
        semaphore = asyncio.Semaphore(self.max_concurrent_api_calls)
        
        async def generate_single(email: Dict[str, Any]):
            async with semaphore:
                try:
                    embedding = await self.generate_embedding(
                        document_id=email['document_id'],
                        email_id=email['email_id'],
                        text=email['text'],
                        metadata=email['metadata']
                    )
                    return embedding
                except Exception as e:
                    logger.error(f"Failed to generate embedding for email {email['email_id']}: {str(e)}")
                    return None
        
        # 병렬 처리
        tasks = [generate_single(email) for email in email_batch]
        results = await asyncio.gather(*tasks)
        
        # None 제거
        embeddings = [emb for emb in results if emb is not None]
        
        return embeddings
    
    async def generate_embedding(
        self, 
        document_id: str,
        email_id: str,
        text: str,
        metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """단일 이메일 임베딩 생성 (폴백용)"""
        try:
            response = await self.client.embeddings.create(
                model=self.model,
                input=text
            )
            
            return {
                'embedding_id': self._generate_embedding_id(document_id),
                'document_id': document_id,
                'email_id': email_id,
                'embedding_vector': response.data[0].embedding,
                'embedding_text': text[:self.text_limit],
                'metadata': {
                    **metadata,
                    'embedding_model': self.model,
                    'vector_dimension': len(response.data[0].embedding)
                }
            }
            
        except Exception as e:
            logger.error(f"Failed to generate embedding: {str(e)}")
            raise
    
    def _generate_embedding_id(self, document_id: str) -> str:
        """임베딩 ID 생성"""
        content = f"emb:{document_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]
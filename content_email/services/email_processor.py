# content_email/services/email_processor.py
import logging
import asyncio
from typing import List, Dict, Any

from ..schema import EmailInfo
from ..utils.email_helpers import extract_email_content, create_email_info

logger = logging.getLogger(__name__)

class EmailProcessor:
    """이메일 처리 서비스 - 배치 처리 로직"""
    
    async def process_emails_with_embeddings(
        self,
        emails: List[Dict[str, Any]],
        account_id: str,
        event_id: str,
        batch_size: int,
        max_concurrent: int,
        embedding_service,
        attachment_service,
        storage_service
    ) -> Dict[str, Any]:
        """이메일 배치 처리 및 임베딩 생성"""
        
        # 배치 생성
        batches = [
            emails[i:i + batch_size] 
            for i in range(0, len(emails), batch_size)
        ]
        
        logger.info(f"Created {len(batches)} batches of {batch_size} emails")
        
        # 동시성 제어
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_batch(batch: List[Dict[str, Any]], batch_idx: int):
            async with semaphore:
                return await self._process_single_batch(
                    batch, batch_idx, account_id, event_id,
                    embedding_service, attachment_service, storage_service
                )
        
        # 모든 배치 병렬 처리
        tasks = [process_batch(batch, idx) for idx, batch in enumerate(batches)]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # 결과 집계
        return self._aggregate_results(batch_results)
    
    async def _process_single_batch(
        self, 
        batch: List[Dict[str, Any]], 
        batch_idx: int,
        account_id: str,
        event_id: str,
        embedding_service,
        attachment_service,
        storage_service
    ) -> Dict[str, Any]:
        """단일 배치 처리"""
        logger.info(f"Processing batch {batch_idx + 1} with {len(batch)} emails")
        
        processed_emails = []
        total_attachments = 0
        downloaded_attachments = 0
        
        # 1. 이메일 문서 저장
        email_documents = []
        for email_data in batch:
            try:
                document_id = await storage_service.save_email_document(
                    email_data=email_data,
                    account_id=account_id,
                    event_id=event_id
                )
                email_documents.append({
                    'email_data': email_data,
                    'document_id': document_id
                })
            except Exception as e:
                logger.error(f"Failed to save email: {str(e)}")
                continue
        
        # 2. 임베딩 생성 및 저장
        if email_documents:
            embeddings = await self._generate_and_save_embeddings(
                email_documents, account_id, embedding_service, storage_service
            )
        else:
            embeddings = []
        
        # 3. 첨부파일 처리
        for doc in email_documents:
            email_data = doc['email_data']
            document_id = doc['document_id']
            
            # 첨부파일 처리
            attachment_infos = []
            if email_data.get('has_attachments', False):
                attachments = email_data.get('attachments', [])
                total_attachments += len(attachments)
                
                for attachment in attachments:
                    attachment_info = await attachment_service.process_attachment(
                        attachment=attachment,
                        email_id=email_data['id'],
                        document_id=document_id,
                        account_id=account_id
                    )
                    
                    await storage_service.save_attachment_info(
                        attachment_info=attachment_info,
                        document_id=document_id,
                        email_id=email_data['id']
                    )
                    
                    attachment_infos.append(attachment_info)
                    if attachment_info.is_downloaded:
                        downloaded_attachments += 1
            
            # 문서 상태 업데이트
            await storage_service.update_document_status(document_id, 'completed')
            
            # 처리된 이메일 정보 생성
            embedding_id = self._find_embedding_id(embeddings, document_id)
            email_info = create_email_info(
                email_data, document_id, embedding_id, attachment_infos
            )
            processed_emails.append(email_info)
        
        return {
            'processed_emails': processed_emails,
            'total_attachments': total_attachments,
            'downloaded_attachments': downloaded_attachments
        }
    
    async def _generate_and_save_embeddings(
        self, 
        email_documents: List[Dict[str, Any]], 
        account_id: str,
        embedding_service,
        storage_service
    ) -> List[Dict[str, Any]]:
        """임베딩 생성 및 저장"""
        # 임베딩할 텍스트 준비
        emails_for_embedding = []
        for doc in email_documents:
            content = extract_email_content(doc['email_data'])
            if content:
                emails_for_embedding.append({
                    'document_id': doc['document_id'],
                    'email_id': doc['email_data']['id'],
                    'text': content,
                    'metadata': {
                        'subject': doc['email_data'].get('subject'),
                        'sender': doc['email_data'].get('from_address', {}).get('emailAddress', {}).get('address'),
                        'received_date': doc['email_data'].get('received_date_time'),
                        'account_id': account_id
                    }
                })
        
        if not emails_for_embedding:
            return []
        
        # 임베딩 생성
        embeddings = await embedding_service.generate_batch_embeddings(emails_for_embedding)
        
        # 저장
        if embeddings:
            await storage_service.save_embeddings_batch(embeddings)
        
        return embeddings
    
    def _find_embedding_id(self, embeddings: List[Dict[str, Any]], document_id: str) -> str:
        """문서 ID에 해당하는 임베딩 ID 찾기"""
        for emb in embeddings:
            if emb['document_id'] == document_id:
                return emb['embedding_id']
        return None
    
    def _aggregate_results(self, batch_results: List) -> Dict[str, Any]:
        """배치 결과 집계"""
        processed_emails = []
        total_attachments = 0
        downloaded_attachments = 0
        
        for result in batch_results:
            if isinstance(result, Exception):
                logger.error(f"Batch processing failed: {str(result)}")
                continue
            
            processed_emails.extend(result['processed_emails'])
            total_attachments += result['total_attachments']
            downloaded_attachments += result['downloaded_attachments']
        
        return {
            'processed_emails': processed_emails,
            'total_attachments': total_attachments,
            'downloaded_attachments': downloaded_attachments
        }
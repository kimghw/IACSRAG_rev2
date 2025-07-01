# content_email/orchestrator.py
import logging
import time
from typing import Dict, Any, List
import asyncio

from infra.core.config import settings
from .schema import EmailProcessingRequest, EmailProcessingResult, EmailInfo, EmailAttachmentInfo
from .email_embedder import EmailEmbedder
from .email_attachment import EmailAttachmentProcessor
from .email_repository import EmailRepository

logger = logging.getLogger(__name__)

class EmailOrchestrator:
    """이메일 처리 오케스트레이터 - 배치 처리 지원"""
    
    def __init__(self):
        self.embedder = EmailEmbedder()
        self.attachment_processor = EmailAttachmentProcessor()
        self.repository = EmailRepository()
        
        # 설정에서 배치 처리 설정 가져오기
        self.batch_size = settings.EMAIL_BATCH_SIZE
        self.max_concurrent_batches = settings.EMAIL_MAX_CONCURRENT_BATCHES
    
    async def process_email(self, request: EmailProcessingRequest) -> EmailProcessingResult:
        """이메일 이벤트 처리 메인 로직 - 배치 처리"""
        start_time = time.time()
        
        try:
            logger.info(f"Starting email processing for event: {request.event_id}")
            
            # 1. 이벤트 데이터에서 이메일 목록 가져오기
            response_data = request.event_data.get('response_data', {})
            emails = response_data.get('value', [])
            
            logger.info(f"Processing {len(emails)} emails in batches of {self.batch_size}")
            
            # 2. 배치 생성
            batches = self._create_batches(emails)
            logger.info(f"Created {len(batches)} batches")
            
            # 3. 배치별 병렬 처리
            processed_emails = []
            total_attachments = 0
            downloaded_attachments = 0
            
            # 동시 처리 제한을 위한 세마포어
            semaphore = asyncio.Semaphore(self.max_concurrent_batches)
            
            async def process_batch(batch: List[Dict[str, Any]], batch_idx: int):
                async with semaphore:
                    batch_result = await self._process_email_batch(
                        batch=batch,
                        batch_idx=batch_idx,
                        account_id=request.account_id,
                        event_id=request.event_id
                    )
                    return batch_result
            
            # 모든 배치 병렬 처리
            batch_tasks = [
                process_batch(batch, idx) 
                for idx, batch in enumerate(batches)
            ]
            
            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # 결과 집계
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Batch processing failed: {str(result)}")
                    continue
                
                processed_emails.extend(result['processed_emails'])
                total_attachments += result['total_attachments']
                downloaded_attachments += result['downloaded_attachments']
            
            processing_time = time.time() - start_time
            
            return EmailProcessingResult(
                event_id=request.event_id,
                account_id=request.account_id,
                email_count=len(emails),
                processed_emails=processed_emails,
                total_attachments=total_attachments,
                downloaded_attachments=downloaded_attachments,
                processing_time=processing_time
            )
            
        except Exception as e:
            logger.error(f"Email processing failed: {str(e)}", exc_info=True)
            raise
    
    def _create_batches(self, emails: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """이메일 목록을 배치로 분할"""
        batches = []
        for i in range(0, len(emails), self.batch_size):
            batch = emails[i:i + self.batch_size]
            batches.append(batch)
        return batches
    
    async def _process_email_batch(
        self, 
        batch: List[Dict[str, Any]], 
        batch_idx: int,
        account_id: str,
        event_id: str
    ) -> Dict[str, Any]:
        """단일 배치 처리"""
        logger.info(f"Processing batch {batch_idx + 1} with {len(batch)} emails")
        
        processed_emails = []
        total_attachments = 0
        downloaded_attachments = 0
        
        # 1. 모든 이메일을 MongoDB에 먼저 저장
        email_documents = []
        for email_data in batch:
            try:
                document_id = await self.repository.save_email_document(
                    email_data=email_data,
                    account_id=account_id,
                    event_id=event_id
                )
                
                email_documents.append({
                    'email_data': email_data,
                    'document_id': document_id
                })
                
            except Exception as e:
                logger.error(f"Failed to save email document: {str(e)}")
                continue
        
        # 2. 배치로 임베딩 생성 (본문이 있는 이메일만)
        emails_for_embedding = []
        for doc in email_documents:
            body_content = doc['email_data'].get('body', {}).get('content', '')
            if body_content:
                emails_for_embedding.append({
                    'document_id': doc['document_id'],
                    'email_id': doc['email_data']['id'],
                    'text': body_content,
                    'metadata': {
                        'subject': doc['email_data'].get('subject'),
                        'sender': doc['email_data'].get('from_address', {}).get('emailAddress', {}).get('address'),
                        'received_date': doc['email_data'].get('received_date_time'),
                        'account_id': account_id
                    }
                })
        
        # 배치 임베딩 생성
        embeddings = []
        if emails_for_embedding:
            embeddings = await self.embedder.generate_batch_embeddings(emails_for_embedding)
            
            # 임베딩 배치 저장
            await self.repository.save_embeddings_batch(embeddings)
        
        # 3. 각 이메일의 첨부파일 처리 및 최종 정보 생성
        for doc in email_documents:
            email_data = doc['email_data']
            document_id = doc['document_id']
            
            # 임베딩 ID 찾기
            embedding_id = None
            for emb in embeddings:
                if emb['document_id'] == document_id:
                    embedding_id = emb['embedding_id']
                    break
            
            # 첨부파일 처리
            attachment_infos = []
            if email_data.get('has_attachments', False):
                attachments = email_data.get('attachments', [])
                total_attachments += len(attachments)
                
                for attachment in attachments:
                    attachment_info = await self.attachment_processor.process_attachment(
                        attachment=attachment,
                        email_id=email_data['id'],
                        document_id=document_id,
                        account_id=account_id
                    )
                    
                    await self.repository.save_attachment_info(
                        attachment_info=attachment_info,
                        document_id=document_id,
                        email_id=email_data['id']
                    )
                    attachment_infos.append(attachment_info)
                    
                    if attachment_info.is_downloaded:
                        downloaded_attachments += 1
            
            # 문서 상태 업데이트
            await self.repository.update_document_status(document_id, 'completed')
            
            # 처리된 이메일 정보
            body_content = email_data.get('body', {}).get('content', '')
            processed_emails.append(EmailInfo(
                email_id=email_data['id'],
                document_id=document_id,
                subject=email_data.get('subject', ''),
                sender=email_data.get('from_address', {}).get('emailAddress', {}).get('address', ''),
                received_datetime=email_data.get('received_date_time', ''),
                body_preview=body_content[:200] + '...' if len(body_content) > 200 else body_content,
                embedding_id=embedding_id,
                is_read=email_data.get('is_read', False),
                has_attachments=email_data.get('has_attachments', False),
                attachments=attachment_infos,
                importance=email_data.get('importance', 'normal')
            ))
        
        logger.info(f"Batch {batch_idx + 1} completed: {len(processed_emails)} emails processed")
        
        return {
            'processed_emails': processed_emails,
            'total_attachments': total_attachments,
            'downloaded_attachments': downloaded_attachments
        }
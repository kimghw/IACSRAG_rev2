# content_email/services/email_storage_service.py
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime

from infra.databases.mongo_db import MongoDB
from infra.databases.qdrant_db import QdrantDB
from ..schema import EmailAttachmentInfo
from ..utils.email_helpers import generate_document_id, generate_embedding_id

logger = logging.getLogger(__name__)

class EmailStorageService:
    """이메일 데이터 저장 서비스"""
    
    def __init__(self):
        self.mongo = MongoDB()
        self.qdrant = QdrantDB()
    
    async def save_email_document(self, email_data: Dict[str, Any], account_id: str, event_id: str) -> str:
        """이메일을 MongoDB 문서로 저장"""
        try:
            document_id = generate_document_id(email_data['id'])
            
            document = {
                'document_id': document_id,
                'document_type': 'email',
                'email_id': email_data.get('id'),
                'account_id': account_id,
                'event_id': event_id,
                'subject': email_data.get('subject'),
                'sender': email_data.get('from_address', {}).get('emailAddress', {}).get('address'),
                'received_datetime': email_data.get('received_date_time'),
                'content': email_data.get('body', {}).get('content'),
                'metadata': {
                    'is_read': email_data.get('is_read'),
                    'has_attachments': email_data.get('has_attachments'),
                    'importance': email_data.get('importance'),
                    'keywords': email_data.get('keywords', [])
                },
                'created_at': datetime.utcnow(),
                'status': 'processing',
                'embedding_id': None,
                'attachments': []
            }
            
            await self.mongo.db.email_documents.insert_one(document)
            logger.info(f"Saved email document: {document_id}")
            
            return document_id
            
        except Exception as e:
            logger.error(f"Failed to save email document: {str(e)}")
            raise
    
    async def save_embeddings_batch(self, embeddings: List[Dict[str, Any]]) -> None:
        """여러 임베딩을 배치로 저장"""
        try:
            if not embeddings:
                return
            
            # Qdrant에 벡터 저장
            points = []
            for embedding in embeddings:
                point = {
                    'id': embedding['embedding_id'],
                    'vector': embedding['embedding_vector'],
                    'payload': {
                        'document_id': embedding['document_id'],
                        'email_id': embedding['email_id'],
                        'text': embedding['embedding_text'],
                        'document_type': 'email',
                        **embedding['metadata']
                    }
                }
                points.append(point)
            
            await self.qdrant.upsert_points(points)
            logger.info(f"Saved {len(points)} embeddings to Qdrant")
            
            # MongoDB 메타데이터 저장
            embedding_docs = []
            for embedding in embeddings:
                embedding_docs.append({
                    '_id': embedding['embedding_id'],
                    'document_id': embedding['document_id'],
                    'email_id': embedding['email_id'],
                    'vector_dimension': len(embedding['embedding_vector']),
                    'metadata': embedding['metadata'],
                    'created_at': datetime.utcnow()
                })
            
            if embedding_docs:
                await self.mongo.db.email_embeddings.insert_many(embedding_docs)
            
            # email_documents에 embedding_id 업데이트
            for embedding in embeddings:
                await self.mongo.db.email_documents.update_one(
                    {'document_id': embedding['document_id']},
                    {'$set': {'embedding_id': embedding['embedding_id']}}
                )
            
            logger.info(f"Updated MongoDB with embedding metadata")
            
        except Exception as e:
            logger.error(f"Failed to save embeddings batch: {str(e)}")
            raise
    
    async def save_attachment_info(
        self, 
        attachment_info: EmailAttachmentInfo,
        document_id: str,
        email_id: str
    ) -> None:
        """첨부파일 정보 저장"""
        try:
            attachment_doc = {
                '_id': attachment_info.attachment_id,
                'document_id': document_id,
                'email_id': email_id,
                'name': attachment_info.name,
                'content_type': attachment_info.content_type,
                'size': attachment_info.size,
                'file_path': attachment_info.file_path,
                'is_downloaded': attachment_info.is_downloaded,
                'download_error': attachment_info.download_error,
                'created_at': datetime.utcnow()
            }
            
            await self.mongo.db.email_attachments.insert_one(attachment_doc)
            
            # email_documents의 attachments 배열에 추가
            await self.mongo.db.email_documents.update_one(
                {'document_id': document_id},
                {'$push': {'attachments': attachment_info.attachment_id}}
            )
            
            logger.info(f"Saved attachment info: {attachment_info.name}")
            
        except Exception as e:
            logger.error(f"Failed to save attachment info: {str(e)}")
    
    async def update_document_status(self, document_id: str, status: str) -> None:
        """문서 상태 업데이트"""
        try:
            await self.mongo.db.email_documents.update_one(
                {'document_id': document_id},
                {
                    '$set': {
                        'status': status,
                        'updated_at': datetime.utcnow()
                    }
                }
            )
            logger.info(f"Updated document {document_id} status to: {status}")
        except Exception as e:
            logger.error(f"Failed to update document status: {str(e)}")
    
    async def update_event_status(self, event_id: str, status: str, error_message: str = None) -> None:
        """이벤트 상태 업데이트"""
        try:
            update_data = {
                'status': status,
                'updated_at': datetime.utcnow()
            }
            
            if error_message:
                update_data['error_message'] = error_message
            
            # email_events 컬렉션이 있다면 업데이트
            await self.mongo.db.email_events.update_one(
                {'event_id': event_id},
                {'$set': update_data},
                upsert=True
            )
            
        except Exception as e:
            logger.error(f"Failed to update event status: {str(e)}")
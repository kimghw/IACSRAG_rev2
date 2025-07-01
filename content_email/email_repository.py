# content_email/email_repository.py
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import hashlib

from infra.databases.mongo_db import MongoDB
from infra.databases.qdrant_db import QdrantDB
from .schema import EmailAttachmentInfo

logger = logging.getLogger(__name__)

class EmailRepository:
    """이메일 데이터 저장소 - MongoDB와 Qdrant 연동"""
    
    def __init__(self):
        self.mongo = MongoDB()
        self.qdrant = QdrantDB()
    
    async def save_email_document(self, email_data: Dict[str, Any], account_id: str, event_id: str) -> str:
        """이메일을 MongoDB 문서로 저장"""
        try:
            # 문서 ID 생성 (이메일 ID 기반)
            document_id = self._generate_document_id(email_data['id'])
            
            # 문서 데이터 구성
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
                'embedding_id': None,  # 임베딩 생성 후 업데이트
                'attachments': []  # 첨부파일 ID 목록
            }
            
            # MongoDB에 저장
            await self.mongo.db.email_documents.insert_one(document)
            logger.info(f"Saved email document to MongoDB: {document_id}")
            
            return document_id
            
        except Exception as e:
            logger.error(f"Failed to save email document: {str(e)}")
            raise
    
    async def save_embedding(self, embedding: Dict[str, Any]) -> None:
        """임베딩을 Qdrant에 저장하고 MongoDB 문서 업데이트"""
        try:
            # 1. Qdrant에 벡터 저장
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
            
            await self.qdrant.upsert_points([point])
            logger.info(f"Saved embedding to Qdrant: {embedding['embedding_id']}")
            
            # 2. MongoDB에 임베딩 메타데이터 저장
            embedding_doc = {
                '_id': embedding['embedding_id'],
                'document_id': embedding['document_id'],
                'email_id': embedding['email_id'],
                'vector_dimension': len(embedding['embedding_vector']),
                'metadata': embedding['metadata'],
                'created_at': datetime.utcnow()
            }
            
            await self.mongo.db.email_embeddings.insert_one(embedding_doc)
            
            # 3. email_documents에 embedding_id 업데이트
            await self.mongo.db.email_documents.update_one(
                {'document_id': embedding['document_id']},
                {'$set': {'embedding_id': embedding['embedding_id']}}
            )
            
            logger.info(f"Updated MongoDB with embedding metadata")
            
        except Exception as e:
            logger.error(f"Failed to save embedding: {str(e)}")
            raise
    
    async def save_embeddings_batch(self, embeddings: List[Dict[str, Any]]) -> None:
        """여러 임베딩을 배치로 저장"""
        try:
            if not embeddings:
                return
            
            # 1. Qdrant에 벡터 배치 저장
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
            
            # 2. MongoDB에 메타데이터 배치 저장
            embedding_docs = []
            document_updates = []
            
            for embedding in embeddings:
                # 임베딩 문서
                embedding_docs.append({
                    '_id': embedding['embedding_id'],
                    'document_id': embedding['document_id'],
                    'email_id': embedding['email_id'],
                    'vector_dimension': len(embedding['embedding_vector']),
                    'metadata': embedding['metadata'],
                    'created_at': datetime.utcnow()
                })
                
                # 문서 업데이트 준비
                document_updates.append({
                    'document_id': embedding['document_id'],
                    'embedding_id': embedding['embedding_id']
                })
            
            # 배치 삽입
            if embedding_docs:
                await self.mongo.db.email_embeddings.insert_many(embedding_docs)
            
            # 문서 업데이트 (각 문서에 embedding_id 추가)
            for update in document_updates:
                await self.mongo.db.email_documents.update_one(
                    {'document_id': update['document_id']},
                    {'$set': {'embedding_id': update['embedding_id']}}
                )
            
            logger.info(f"Updated MongoDB with {len(embedding_docs)} embedding metadata")
            
        except Exception as e:
            logger.error(f"Failed to save embeddings batch: {str(e)}")
            raise
    
    async def save_attachment_info(
        self, 
        attachment_info: EmailAttachmentInfo,
        document_id: str,
        email_id: str
    ) -> None:
        """첨부파일 정보를 MongoDB에 저장"""
        try:
            # MongoDB에 첨부파일 정보 저장
            attachment_doc = {
                '_id': attachment_info.attachment_id,
                'document_id': document_id,  # MongoDB 연동
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
    
    async def get_email_with_embedding(self, document_id: str) -> Optional[Dict[str, Any]]:
        """MongoDB와 Qdrant에서 이메일 정보와 임베딩을 함께 조회"""
        try:
            # MongoDB에서 이메일 문서 조회
            email_doc = await self.mongo.db.email_documents.find_one({'document_id': document_id})
            
            if not email_doc:
                return None
            
            # Qdrant에서 임베딩 조회
            if email_doc.get('embedding_id'):
                embedding_point = await self.qdrant.get_point(email_doc['embedding_id'])
                if embedding_point:
                    email_doc['embedding'] = embedding_point
            
            # 첨부파일 정보 조회
            if email_doc.get('attachments'):
                attachments = []
                for attachment_id in email_doc['attachments']:
                    attachment = await self.mongo.db.email_attachments.find_one({'_id': attachment_id})
                    if attachment:
                        attachments.append(attachment)
                email_doc['attachment_details'] = attachments
            
            return email_doc
            
        except Exception as e:
            logger.error(f"Failed to get email with embedding: {str(e)}")
            return None
    
    def _generate_document_id(self, email_id: str) -> str:
        """문서 ID 생성"""
        content = f"email:{email_id}"
        return hashlib.sha256(content.encode()).hexdigest()[:24]
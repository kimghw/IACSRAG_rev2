# content_email/utils/email_helpers.py
import hashlib
from typing import Dict, Any, List
from ..schema import EmailInfo

def generate_document_id(email_id: str) -> str:
    """문서 ID 생성"""
    content = f"email:{email_id}"
    return hashlib.sha256(content.encode()).hexdigest()[:24]

def generate_embedding_id(document_id: str) -> str:
    """임베딩 ID 생성"""
    content = f"emb:{document_id}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def generate_attachment_id(email_id: str, attachment_id: str) -> str:
    """첨부파일 ID 생성"""
    content = f"attach:{email_id}:{attachment_id}"
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def extract_email_content(email_data: Dict[str, Any]) -> str:
    """이메일 본문 추출"""
    return email_data.get('body', {}).get('content', '')

def extract_email_address(email_data: Dict[str, Any]) -> str:
    """이메일 주소 추출"""
    return email_data.get('from_address', {}).get('emailAddress', {}).get('address', '')

def sanitize_filename(filename: str) -> str:
    """파일명 안전하게 변환"""
    # 위험한 문자 제거
    safe_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.-_"
    sanitized = "".join(c if c in safe_chars else "_" for c in filename)
    
    # 빈 파일명 방지
    if not sanitized:
        sanitized = "attachment"
    
    return sanitized

def create_email_info(
    email_data: Dict[str, Any], 
    document_id: str,
    embedding_id: str,
    attachment_infos: List
) -> EmailInfo:
    """EmailInfo 객체 생성"""
    body_content = extract_email_content(email_data)
    
    return EmailInfo(
        email_id=email_data['id'],
        document_id=document_id,
        subject=email_data.get('subject', ''),
        sender=extract_email_address(email_data),
        received_datetime=email_data.get('received_date_time', ''),
        body_preview=body_content[:200] + '...' if len(body_content) > 200 else body_content,
        embedding_id=embedding_id,
        is_read=email_data.get('is_read', False),
        has_attachments=email_data.get('has_attachments', False),
        attachments=attachment_infos,
        importance=email_data.get('importance', 'normal')
    )
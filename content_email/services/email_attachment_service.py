# content_email/services/email_attachment_service.py
import logging
import os
import aiofiles
from typing import Dict, Any
from pathlib import Path
import aiohttp

from infra.core.config import settings
from ..schema import EmailAttachmentInfo
from ..utils.email_helpers import generate_attachment_id, sanitize_filename

logger = logging.getLogger(__name__)

class EmailAttachmentService:
    """이메일 첨부파일 처리 서비스"""
    
    def __init__(self):
        self.enabled = settings.EMAIL_ATTACHMENT_ENABLED
        self.max_size = settings.EMAIL_ATTACHMENT_MAX_SIZE
        self.allowed_types = settings.email_attachment_allowed_types_list
        self.attachment_base_path = Path(settings.EMAIL_ATTACHMENT_BASE_PATH)
        self.attachment_base_path.mkdir(parents=True, exist_ok=True)
    
    async def process_attachment(
        self, 
        attachment: Dict[str, Any],
        email_id: str,
        document_id: str,
        account_id: str
    ) -> EmailAttachmentInfo:
        """첨부파일 처리 및 다운로드"""
        try:
            attachment_id = generate_attachment_id(email_id, attachment.get('id', ''))
            
            # 첨부파일 정보 구성
            attachment_info = EmailAttachmentInfo(
                attachment_id=attachment_id,
                name=attachment.get('name', 'unknown'),
                content_type=attachment.get('contentType', 'application/octet-stream'),
                size=attachment.get('size', 0),
                file_path=""
            )
            
            # 처리 비활성화 상태면 정보만 반환
            if not self.enabled:
                attachment_info.download_error = "Attachment processing disabled"
                return attachment_info
            
            # 검증
            validation_error = self._validate_attachment(attachment_info)
            if validation_error:
                attachment_info.download_error = validation_error
                return attachment_info
            
            # 다운로드
            download_url = attachment.get('contentLocation') or attachment.get('@odata.mediaContentType')
            if download_url:
                file_path = await self._create_file_path(
                    account_id=account_id,
                    email_id=email_id,
                    attachment_name=attachment_info.name
                )
                
                success = await self._download_attachment(download_url, file_path)
                
                if success:
                    attachment_info.file_path = str(file_path)
                    attachment_info.is_downloaded = True
                    logger.info(f"Downloaded attachment: {attachment_info.name}")
                else:
                    attachment_info.download_error = "Download failed"
            else:
                attachment_info.download_error = "No download URL available"
            
            return attachment_info
            
        except Exception as e:
            logger.error(f"Failed to process attachment: {str(e)}")
            raise
    
    def _validate_attachment(self, attachment_info: EmailAttachmentInfo) -> str:
        """첨부파일 검증"""
        # 파일 크기 검증
        if attachment_info.size > self.max_size:
            return f"File too large: {attachment_info.size} bytes (max: {self.max_size})"
        
        # 파일 타입 검증
        file_ext = Path(attachment_info.name).suffix.lower().lstrip('.')
        if file_ext not in self.allowed_types:
            return f"File type not allowed: {file_ext}"
        
        return None
    
    async def _download_attachment(self, url: str, file_path: Path) -> bool:
        """첨부파일 다운로드"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        
                        async with aiofiles.open(file_path, 'wb') as f:
                            await f.write(content)
                        
                        return True
                    else:
                        logger.error(f"Download failed: HTTP {response.status}")
                        return False
                        
        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            return False
    
    async def _create_file_path(self, account_id: str, email_id: str, attachment_name: str) -> Path:
        """첨부파일 저장 경로 생성"""
        # 계정별/이메일별 디렉토리 구조
        dir_path = self.attachment_base_path / account_id / email_id
        dir_path.mkdir(parents=True, exist_ok=True)
        
        # 파일명 안전하게 변환
        safe_filename = sanitize_filename(attachment_name)
        file_path = dir_path / safe_filename
        
        # 동일 파일명 존재 시 번호 추가
        if file_path.exists():
            base_name = file_path.stem
            extension = file_path.suffix
            counter = 1
            
            while file_path.exists():
                file_path = dir_path / f"{base_name}_{counter}{extension}"
                counter += 1
        
        return file_path
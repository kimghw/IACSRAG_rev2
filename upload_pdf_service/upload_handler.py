import os
import aiofiles
from typing import Dict, Any

from infra.core.config import settings

class UploadHandler:
    """파일 업로드 핸들러"""
    
    async def save_temp_file(
        self, 
        document_id: str, 
        filename: str, 
        file_content: bytes
    ) -> str:
        """임시 파일 저장"""
        temp_dir = settings.TEMP_UPLOAD_DIR
        os.makedirs(temp_dir, exist_ok=True)
        
        file_path = os.path.join(temp_dir, f"{document_id}_{filename}")
        
        async with aiofiles.open(file_path, 'wb') as f:
            await f.write(file_content)
        
        return file_path
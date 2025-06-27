# upload_service/file_hasher.py
import hashlib
from typing import Tuple
import logging

logger = logging.getLogger(__name__)

class FileHasher:
    """파일 중복 검사를 위한 해시 생성기"""
    
    CHUNK_SIZE = 10 * 1024  # 10KB
    
    @staticmethod
    def generate_quick_hash(filename: str, file_content: bytes) -> str:
        """
        파일의 quick hash 생성
        Hash = SHA256(filename + file_size + first_10KB + last_10KB)
        
        Args:
            filename: 파일명
            file_content: 파일 내용 (bytes)
            
        Returns:
            str: 생성된 해시 문자열
        """
        file_size = len(file_content)
        
        # 파일이 20KB보다 작은 경우 전체 내용 사용
        if file_size <= FileHasher.CHUNK_SIZE * 2:
            first_chunk = file_content
            last_chunk = b""
        else:
            first_chunk = file_content[:FileHasher.CHUNK_SIZE]
            last_chunk = file_content[-FileHasher.CHUNK_SIZE:]
        
        # 해시 생성을 위한 데이터 조합
        hash_data = (
            filename.encode('utf-8') +
            str(file_size).encode('utf-8') +
            first_chunk +
            last_chunk
        )
        
        # SHA256 해시 생성
        hash_object = hashlib.sha256(hash_data)
        quick_hash = hash_object.hexdigest()
        
        logger.debug(f"Generated quick hash for {filename}: {quick_hash[:16]}...")
        
        return quick_hash
    
    @staticmethod
    def get_file_chunks(file_content: bytes) -> Tuple[bytes, bytes]:
        """
        파일의 처음과 마지막 청크 반환 (디버깅용)
        
        Args:
            file_content: 파일 내용
            
        Returns:
            Tuple[bytes, bytes]: (first_chunk, last_chunk)
        """
        file_size = len(file_content)
        
        if file_size <= FileHasher.CHUNK_SIZE * 2:
            return file_content, b""
        else:
            return (
                file_content[:FileHasher.CHUNK_SIZE],
                file_content[-FileHasher.CHUNK_SIZE:]
            )
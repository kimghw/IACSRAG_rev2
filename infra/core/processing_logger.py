# infra/core/processing_logger.py
import logging
import os
from datetime import datetime
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
from pathlib import Path

from .config import settings


class ProcessingLogger:
    """PDF 처리 과정 전용 로거"""
    
    def __init__(self):
        self.logger = None
        self._setup_logger()
    
    def _setup_logger(self):
        """로거 설정"""
        if settings.PROCESSING_LOG_MODE == "none":
            return
        
        self.logger = logging.getLogger("processing")
        self.logger.setLevel(getattr(logging, settings.PROCESSING_LOG_LEVEL))
        
        # 기존 핸들러 제거
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        formatter = logging.Formatter(
            '%(asctime)s - [%(levelname)s] - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # 콘솔 핸들러
        if settings.PROCESSING_LOG_MODE in ["console", "both"]:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        
        # 파일 핸들러
        if settings.PROCESSING_LOG_MODE in ["file", "both"]:
            # 로그 디렉토리 생성
            log_file_path = Path(settings.PROCESSING_LOG_FILE)
            log_file_path.parent.mkdir(parents=True, exist_ok=True)
            
            if settings.PROCESSING_LOG_ROTATION:
                # 로그 로테이션 사용
                file_handler = RotatingFileHandler(
                    settings.PROCESSING_LOG_FILE,
                    maxBytes=self._parse_size(settings.PROCESSING_LOG_MAX_SIZE),
                    backupCount=settings.PROCESSING_LOG_BACKUP_COUNT,
                    encoding='utf-8'
                )
            else:
                # 단순 파일 핸들러
                file_handler = logging.FileHandler(
                    settings.PROCESSING_LOG_FILE,
                    encoding='utf-8'
                )
            
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
    
    def _parse_size(self, size_str: str) -> int:
        """크기 문자열을 바이트로 변환 (예: '10MB' -> 10485760)"""
        size_str = size_str.upper()
        if size_str.endswith('KB'):
            return int(size_str[:-2]) * 1024
        elif size_str.endswith('MB'):
            return int(size_str[:-2]) * 1024 * 1024
        elif size_str.endswith('GB'):
            return int(size_str[:-2]) * 1024 * 1024 * 1024
        else:
            return int(size_str)
    
    def _log(self, level: str, message: str, **kwargs):
        """로그 메시지 출력"""
        if not self.logger or settings.PROCESSING_LOG_MODE == "none":
            return
        
        # 추가 정보가 있으면 메시지에 포함
        if kwargs:
            extra_info = " | ".join([f"{k}={v}" for k, v in kwargs.items()])
            message = f"{message} | {extra_info}"
        
        getattr(self.logger, level.lower())(message)
    
    def upload_started(self, document_id: str, filename: str, file_size: int):
        """업로드 시작 로그"""
        self._log("INFO", f"📤 업로드 시작", 
                 document_id=document_id, 
                 filename=filename, 
                 file_size=f"{file_size:,} bytes")
    
    def upload_completed(self, document_id: str, duration: float):
        """업로드 완료 로그"""
        self._log("INFO", f"📤 업로드 완료", 
                 document_id=document_id, 
                 duration=f"{duration:.2f}s")
    
    def text_extraction_started(self, document_id: str):
        """텍스트 추출 시작 로그"""
        self._log("INFO", f"📄 텍스트 추출 시작", document_id=document_id)
    
    def text_extraction_completed(self, document_id: str, duration: float, text_length: int):
        """텍스트 추출 완료 로그"""
        self._log("INFO", f"📄 텍스트 추출 완료", 
                 document_id=document_id, 
                 duration=f"{duration:.2f}s", 
                 text_length=f"{text_length:,} chars")
    
    def chunking_started(self, document_id: str):
        """청킹 시작 로그"""
        self._log("INFO", f"✂️ 청킹 시작", document_id=document_id)
    
    def chunking_completed(self, document_id: str, duration: float, chunk_count: int):
        """청킹 완료 로그"""
        self._log("INFO", f"✂️ 청킹 완료", 
                 document_id=document_id, 
                 duration=f"{duration:.2f}s", 
                 chunk_count=chunk_count)
    
    def embedding_started(self, document_id: str, chunk_count: int, model: str):
        """임베딩 생성 시작 로그"""
        self._log("INFO", f"🧠 임베딩 생성 시작", 
                 document_id=document_id, 
                 chunk_count=chunk_count, 
                 model=model)
    
    def embedding_completed(self, document_id: str, duration: float, embedding_count: int):
        """임베딩 생성 완료 로그"""
        self._log("INFO", f"🧠 임베딩 생성 완료", 
                 document_id=document_id, 
                 duration=f"{duration:.2f}s", 
                 embedding_count=embedding_count)
    
    def vector_storage_started(self, document_id: str, vector_count: int):
        """벡터 저장 시작 로그"""
        self._log("INFO", f"💾 벡터 저장 시작", 
                 document_id=document_id, 
                 vector_count=vector_count)
    
    def vector_storage_completed(self, document_id: str, duration: float, stored_count: int):
        """벡터 저장 완료 로그"""
        self._log("INFO", f"💾 벡터 저장 완료", 
                 document_id=document_id, 
                 duration=f"{duration:.2f}s", 
                 stored_count=stored_count)
    
    def processing_completed(self, document_id: str, total_duration: float, stats: Dict[str, Any]):
        """전체 처리 완료 로그"""
        self._log("INFO", f"✅ 전체 처리 완료", 
                 document_id=document_id, 
                 total_duration=f"{total_duration:.2f}s",
                 **stats)
    
    def error(self, document_id: str, step: str, error: str):
        """에러 로그"""
        self._log("ERROR", f"❌ 처리 오류", 
                 document_id=document_id, 
                 step=step, 
                 error=error)
    
    def debug(self, message: str, **kwargs):
        """디버그 로그"""
        self._log("DEBUG", message, **kwargs)


# 전역 인스턴스
processing_logger = ProcessingLogger()

# content_pdf/chunking/spacy_chunk_tracker.py
from typing import List
import logging
from ..schema import ChunkDocument

logger = logging.getLogger(__name__)


class ChunkTracker:
    """청크 생성 추적 및 검증"""
    
    def __init__(self, total_text_length: int):
        self.total_text_length = total_text_length
        self.processed_length = 0
        self.last_processed_end = 0
        self.chunks_created = 0
        self.chunk_sizes = []
    
    def track_chunk(self, chunk: ChunkDocument):
        """청크 추적"""
        chunk_length = chunk.char_end - chunk.char_start
        self.processed_length += chunk_length
        self.last_processed_end = chunk.char_end
        self.chunks_created += 1
        self.chunk_sizes.append(chunk_length)
    
    def verify_coverage(self, warning_threshold: float = 0.05):
        """텍스트 커버리지 검증"""
        if self.total_text_length == 0:
            return
        
        loss_ratio = 1 - (self.processed_length / self.total_text_length)
        
        if loss_ratio > warning_threshold:
            loss_percentage = loss_ratio * 100
            logger.warning(
                f"⚠️ Potential text loss detected in SpaCy chunking: "
                f"{self.processed_length}/{self.total_text_length} chars processed "
                f"({loss_percentage:.1f}% loss)"
            )
            
            # 손실된 텍스트 범위 정보
            if self.last_processed_end < self.total_text_length:
                logger.warning(
                    f"Lost text range: positions {self.last_processed_end} to {self.total_text_length}"
                )
    
    def log_summary(self, document_id: str):
        """청킹 요약 정보 로그"""
        coverage_percentage = (self.processed_length / self.total_text_length * 100) if self.total_text_length > 0 else 0
        avg_chunk_size = sum(self.chunk_sizes) / len(self.chunk_sizes) if self.chunk_sizes else 0
        
        logger.info(f"=== SpaCy Chunking Summary ===")
        logger.info(f"Document ID: {document_id}")
        logger.info(f"Total text length: {self.total_text_length} chars")
        logger.info(f"Processed length: {self.processed_length} chars")
        logger.info(f"Text coverage: {coverage_percentage:.1f}%")
        logger.info(f"Total chunks created: {self.chunks_created}")
        logger.info(f"Average chunk size: {avg_chunk_size:.0f} chars")
        
        if self.chunk_sizes:
            logger.info(f"Min chunk size: {min(self.chunk_sizes)} chars")
            logger.info(f"Max chunk size: {max(self.chunk_sizes)} chars")
        
        logger.info(f"==============================")
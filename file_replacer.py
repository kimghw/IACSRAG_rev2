#!/usr/bin/env python3
"""
replace 폴더의 파일들을 첫 부분의 경로 정보를 읽어서 해당 경로로 파일을 이동/복사하는 스크립트
"""

import os
import shutil
import re
import time
from pathlib import Path
from typing import List, Tuple, Optional

def extract_file_path_from_content(file_path: str) -> Optional[str]:
    """
    파일 내용의 첫 부분에서 경로 정보를 추출합니다.
    
    Args:
        file_path: 읽을 파일 경로
        
    Returns:
        추출된 경로 또는 None
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # 첫 10줄만 읽어서 경로 정보 찾기
            lines = [f.readline().strip() for _ in range(10)]
            
        # 다양한 패턴으로 경로 찾기
        patterns = [
            r'^#\s*파일\s*경로\s*:\s*(.+)$',  # # 파일 경로: path/to/file
            r'^#\s*PATH\s*:\s*(.+)$',         # # PATH: path/to/file
            r'^#\s*File\s*:\s*(.+)$',         # # File: path/to/file
            r'^#\s*Target\s*:\s*(.+)$',       # # Target: path/to/file
            r'^#\s*Replace\s*:\s*(.+)$',      # # Replace: path/to/file
            r'^#\s*(.+\.py)$',                # # some/path/file.py
            r'^#\s*(.+\..+)$',                # # some/path/file.ext
            r'^\s*#\s*(.+/[^/]+\.[^/]+)$',    # # path/file.ext
        ]
        
        for line in lines:
            if not line or not line.startswith('#'):
                continue
                
            for pattern in patterns:
                match = re.match(pattern, line, re.IGNORECASE)
                if match:
                    target_path = match.group(1).strip()
                    # 상대 경로를 절대 경로로 변환
                    if not os.path.isabs(target_path):
                        target_path = os.path.join(os.getcwd(), target_path)
                    return target_path
                    
    except Exception as e:
        print(f"파일 {file_path} 읽기 오류: {e}")
        
    return None

def find_replace_files(directory: str = ".") -> List[str]:
    """
    /home/kimghw/Downloads 폴더의 *.py 파일들 중 최근 3분 안에 생성된 파일들을 찾습니다.
    
    Args:
        directory: 검색할 디렉토리 (사용되지 않음)
        
    Returns:
        교체할 파일들의 경로 리스트
    """
    replace_files = []
    
    # 현재 시간에서 3분 전 시간 계산
    three_minutes_ago = time.time() - (3 * 60)  # 3분 = 180초
    
    # /home/kimghw/Downloads 폴더의 *.py 파일들 찾기
    downloads_dir = "/home/kimghw/Downloads"
    if os.path.exists(downloads_dir) and os.path.isdir(downloads_dir):
        for file in os.listdir(downloads_dir):
            if file.endswith('.py') and not file.startswith('.'):
                file_path = os.path.join(downloads_dir, file)
                if os.path.isfile(file_path):
                    # 파일 생성 시간 확인
                    file_creation_time = os.path.getctime(file_path)
                    if file_creation_time >= three_minutes_ago:
                        replace_files.append(file_path)
                        print(f"  📅 최근 파일 발견: {file} (생성시간: {time.ctime(file_creation_time)})")
                
    return replace_files

def process_file_replacement(source_file: str, dry_run: bool = False) -> bool:
    """
    단일 파일을 처리하여 지정된 경로로 이동/복사합니다.
    
    Args:
        source_file: 원본 파일 경로
        dry_run: True면 실제 작업 없이 시뮬레이션만
        
    Returns:
        성공 여부
    """
    print(f"\n처리 중: {source_file}")
    
    # 파일에서 대상 경로 추출
    target_path = extract_file_path_from_content(source_file)
    
    if not target_path:
        print(f"  ❌ 대상 경로를 찾을 수 없습니다.")
        return False
        
    print(f"  📍 대상 경로: {target_path}")
    
    # 대상 디렉토리 생성
    target_dir = os.path.dirname(target_path)
    
    if dry_run:
        print(f"  🔍 [DRY RUN] 디렉토리 생성: {target_dir}")
        print(f"  🔍 [DRY RUN] 파일 복사: {source_file} -> {target_path}")
        return True
        
    try:
        # 디렉토리가 없으면 생성 (중첩 디렉토리도 모두 생성)
        if target_dir and not os.path.exists(target_dir):
            os.makedirs(target_dir, exist_ok=True)
            print(f"  📁 디렉토리 생성: {target_dir}")
            
        # 파일 복사
        shutil.copy2(source_file, target_path)
        print(f"  ✅ 파일 복사 완료: {target_path}")
        
        # 원본 파일 삭제
        os.remove(source_file)
        print(f"  🗑️ 원본 파일 삭제: {source_file}")
        
        return True
        
    except Exception as e:
        print(f"  ❌ 오류 발생: {e}")
        return False

def main():
    """메인 함수"""
    print("🔄 파일 교체 스크립트 시작")
    print("=" * 50)
    
    # 교체할 파일들 찾기
    replace_files = find_replace_files()
    
    if not replace_files:
        print("❌ 교체할 파일을 찾을 수 없습니다.")
        print("replace/ 폴더의 파일들이나 'replace_'로 시작하거나 '_replace.py', '_replace.txt'로 끝나는 파일을 찾습니다.")
        return
        
    print(f"📋 발견된 교체 파일: {len(replace_files)}개")
    for file in replace_files:
        print(f"  - {file}")
        
    # 사용자 확인
    print("\n🔍 DRY RUN 모드로 먼저 확인합니다...")
    print("-" * 30)
    
    success_count = 0
    for file in replace_files:
        if process_file_replacement(file, dry_run=True):
            success_count += 1
            
    print(f"\n📊 DRY RUN 결과: {success_count}/{len(replace_files)} 파일 처리 가능")
    
    if success_count == 0:
        print("❌ 처리 가능한 파일이 없습니다.")
        return
        
    # 실제 실행 확인
    response = input(f"\n실제로 {success_count}개 파일을 처리하시겠습니까? (y/N): ").strip().lower()
    
    if response not in ['y', 'yes']:
        print("❌ 작업이 취소되었습니다.")
        return
        
    print("\n🚀 실제 파일 처리 시작...")
    print("-" * 30)
    
    final_success = 0
    for file in replace_files:
        if process_file_replacement(file, dry_run=False):
            final_success += 1
            
    print(f"\n🎉 작업 완료: {final_success}/{len(replace_files)} 파일 처리됨")

if __name__ == "__main__":
    main()

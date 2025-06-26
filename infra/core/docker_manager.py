# infra/core/docker_manager.py
import subprocess
import logging
from typing import Dict, Optional, List
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class DockerManager:
    """Docker 컨테이너 관리 기본 클래스"""
    
    @staticmethod
    def is_docker_running() -> bool:
        """Docker 데몬 실행 확인"""
        try:
            subprocess.run(
                ['docker', 'info'], 
                stdout=subprocess.DEVNULL, 
                stderr=subprocess.DEVNULL, 
                check=True
            )
            return True
        except:
            logger.error("Docker is not running")
            return False
    
    @staticmethod
    def get_container_status(container_name: str) -> Optional[str]:
        """컨테이너 상태 확인"""
        try:
            result = subprocess.run(
                ['docker', 'inspect', '-f', '{{.State.Status}}', container_name],
                capture_output=True, 
                text=True
            )
            if result.returncode == 0:
                return result.stdout.strip()
            return None
        except Exception as e:
            logger.debug(f"Container {container_name} not found: {e}")
            return None
    
    @staticmethod
    def start_container(container_name: str) -> bool:
        """컨테이너 시작"""
        try:
            subprocess.run(['docker', 'start', container_name], check=True)
            logger.info(f"Started container: {container_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to start container {container_name}: {e}")
            return False
    
    @staticmethod
    def create_container(service_name: str) -> bool:
        """docker-compose로 컨테이너 생성"""
        try:
            subprocess.run(['docker-compose', 'up', '-d', service_name], check=True)
            logger.info(f"Created container for service: {service_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create container for {service_name}: {e}")
            return False

class ServiceInitializer(ABC):
    """서비스 초기화 추상 클래스"""
    
    def __init__(self, container_name: str, service_name: str):
        self.container_name = container_name
        self.service_name = service_name
        self.docker = DockerManager()
    
    def ensure_container_running(self) -> bool:
        """컨테이너 실행 보장"""
        status = self.docker.get_container_status(self.container_name)
        
        if status == 'running':
            logger.info(f"{self.service_name} is already running")
            return True
        elif status in ['exited', 'stopped']:
            logger.info(f"{self.service_name} is stopped. Starting...")
            return self.docker.start_container(self.container_name)
        else:
            logger.info(f"{self.service_name} container not found. Creating...")
            return self.docker.create_container(self.service_name)
    
    @abstractmethod
    async def test_connection(self) -> bool:
        """연결 테스트 - 서브클래스에서 구현"""
        pass
    
    @abstractmethod
    async def initialize(self) -> bool:
        """서비스별 초기화 - 서브클래스에서 구현"""
        pass
    
    async def setup(self) -> bool:
        """전체 설정 프로세스"""
        # 1. 컨테이너 실행 확인
        if not self.ensure_container_running():
            return False
        
        # 2. 연결 대기
        import asyncio
        for attempt in range(10):
            if await self.test_connection():
                logger.info(f"{self.service_name} connection established")
                break
            if attempt < 9:
                logger.info(f"Waiting for {self.service_name}... ({attempt + 1}/10)")
                await asyncio.sleep(3)
        else:
            logger.error(f"{self.service_name} connection failed")
            return False
        
        # 3. 초기화
        return await self.initialize()
# infra/events/kafka_initializer.py
import logging
import subprocess
import socket
import asyncio
from typing import List
from infra.core.docker_manager import ServiceInitializer
from infra.core.config import settings

logger = logging.getLogger(__name__)

class ZookeeperInitializer(ServiceInitializer):
    """Zookeeper 초기화 관리"""
    
    def __init__(self):
        super().__init__('iacsrag_zookeeper', 'zookeeper')
    
    async def test_connection(self) -> bool:
        """Zookeeper 연결 테스트 - 포트 확인 방식"""
        try:
            # Zookeeper 포트(2181)가 열려있는지 확인
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', 2181))
            sock.close()
            
            if result == 0:
                logger.debug("Zookeeper port 2181 is open")
                return True
            else:
                logger.debug("Zookeeper port 2181 is not accessible")
                return False
                
        except Exception as e:
            logger.debug(f"Zookeeper connection test failed: {e}")
            return False
    
    async def initialize(self) -> bool:
        """Zookeeper는 별도 초기화 불필요"""
        logger.info("Zookeeper requires no additional initialization")
        return True


class KafkaInitializer(ServiceInitializer):
    """Kafka 초기화 관리"""
    
    def __init__(self):
        super().__init__('iacsrag_kafka', 'kafka')
        self.topics = [
            settings.KAFKA_TOPIC_DOCUMENT_UPLOADED,
            settings.KAFKA_TOPIC_TEXT_EXTRACTED,
            settings.KAFKA_TOPIC_CHUNKS_CREATED,
            settings.KAFKA_TOPIC_EMBEDDINGS_GENERATED,
            settings.KAFKA_TOPIC_EMAIL_RECEIVED
        ]
    
    async def test_connection(self) -> bool:
        """Kafka 연결 테스트 - 포트 확인 후 브로커 확인"""
        try:
            # 1. Kafka 포트(9092)가 열려있는지 확인
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            
            if result != 0:
                logger.debug("Kafka port 9092 is not accessible")
                return False
            
            logger.debug("Kafka port 9092 is open, checking broker...")
            
            # 2. Kafka 브로커가 실제로 응답하는지 확인
            # kafka-broker-api-versions 명령을 사용하여 확인
            try:
                result = subprocess.run(
                    ['docker', 'exec', self.container_name, 
                     'kafka-broker-api-versions', '--bootstrap-server', 'localhost:9092'],
                    capture_output=True,
                    timeout=5
                )
                
                if result.returncode == 0:
                    logger.debug("Kafka broker is responding")
                    return True
                else:
                    logger.debug(f"Kafka broker check failed: {result.stderr.decode()}")
                    return False
                    
            except subprocess.TimeoutExpired:
                logger.debug("Kafka broker check timed out")
                return False
            except Exception as e:
                logger.debug(f"Kafka broker check error: {e}")
                # 포트는 열려있지만 브로커 확인 실패 - 일단 True 반환
                return True
                
        except Exception as e:
            logger.debug(f"Kafka connection test failed: {e}")
            return False
    
    async def initialize(self) -> bool:
        """Kafka 토픽 초기화"""
        try:
            # Kafka가 완전히 시작될 때까지 잠시 대기
            logger.info("Waiting for Kafka to be fully ready...")
            await asyncio.sleep(3)
            
            # 현재 토픽 목록 조회
            logger.info("Checking existing topics...")
            result = subprocess.run(
                ['docker', 'exec', self.container_name,
                 'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to list topics: {result.stderr}")
                return False
            
            existing_topics = [topic.strip() for topic in result.stdout.strip().split('\n') if topic.strip()]
            logger.info(f"Existing topics: {existing_topics}")
            
            # 필요한 토픽 생성
            created_count = 0
            for topic in self.topics:
                if topic not in existing_topics:
                    logger.info(f"Creating topic: {topic}")
                    try:
                        result = subprocess.run(
                            ['docker', 'exec', self.container_name,
                             'kafka-topics', '--create',
                             '--topic', topic,
                             '--bootstrap-server', 'localhost:9092',
                             '--partitions', '3',
                             '--replication-factor', '1'],
                            capture_output=True,
                            text=True,
                            timeout=10
                        )
                        
                        if result.returncode == 0:
                            logger.info(f"✅ Created topic: {topic}")
                            created_count += 1
                        else:
                            logger.error(f"Failed to create topic {topic}: {result.stderr}")
                            # 토픽이 이미 존재하는 경우일 수 있으므로 계속 진행
                            
                    except subprocess.TimeoutExpired:
                        logger.error(f"Timeout creating topic: {topic}")
                        return False
                else:
                    logger.info(f"✅ Topic already exists: {topic}")
            
            logger.info(f"Kafka initialization completed. Created {created_count} new topics.")
            return True
            
        except Exception as e:
            logger.error(f"Kafka initialization failed: {e}")
            return False
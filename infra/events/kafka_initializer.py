# infra/events/kafka_initializer.py
import logging
import subprocess
from typing import List
from infra.core.docker_manager import ServiceInitializer
from infra.core.config import settings

logger = logging.getLogger(__name__)

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
        """Kafka 연결 테스트"""
        try:
            result = subprocess.run(
                ['docker', 'exec', self.container_name, 
                 'kafka-broker-api-versions', '--bootstrap-server', 'localhost:9092'],
                capture_output=True,
                timeout=10
            )
            return result.returncode == 0
        except Exception as e:
            logger.debug(f"Kafka connection test failed: {e}")
            return False
    
    async def initialize(self) -> bool:
        """Kafka 토픽 초기화"""
        try:
            # 현재 토픽 목록 조회
            result = subprocess.run(
                ['docker', 'exec', self.container_name,
                 'kafka-topics', '--list', '--bootstrap-server', 'localhost:9092'],
                capture_output=True,
                text=True
            )
            existing_topics = result.stdout.strip().split('\n') if result.returncode == 0 else []
            
            # 필요한 토픽 생성
            for topic in self.topics:
                if topic not in existing_topics:
                    subprocess.run(
                        ['docker', 'exec', self.container_name,
                         'kafka-topics', '--create',
                         '--topic', topic,
                         '--bootstrap-server', 'localhost:9092',
                         '--partitions', '3',
                         '--replication-factor', '1'],
                        check=True
                    )
                    logger.info(f"Created topic: {topic}")
                else:
                    logger.info(f"Topic already exists: {topic}")
            
            return True
            
        except Exception as e:
            logger.error(f"Kafka initialization failed: {e}")
            return False

class ZookeeperInitializer(ServiceInitializer):
    """Zookeeper 초기화 관리"""
    
    def __init__(self):
        super().__init__('iacsrag_zookeeper', 'zookeeper')
    
    async def test_connection(self) -> bool:
        """Zookeeper 연결 테스트"""
        try:
            result = subprocess.run(
                ['docker', 'exec', self.container_name, 
                 'zkServer.sh', 'status'],
                capture_output=True
            )
            return 'Mode:' in result.stdout.decode()
        except:
            return False
    
    async def initialize(self) -> bool:
        """Zookeeper는 별도 초기화 불필요"""
        return True
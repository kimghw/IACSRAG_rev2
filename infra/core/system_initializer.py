# infra/core/system_initializer.py
import logging
import asyncio
from typing import List, Tuple
from infra.core.docker_manager import DockerManager
from infra.databases.mongo_initializer import MongoInitializer
from infra.databases.qdrant_initializer import QdrantInitializer
from infra.events.kafka_initializer import KafkaInitializer, ZookeeperInitializer

logger = logging.getLogger(__name__)

class SystemInitializer:
    """전체 시스템 초기화 관리"""
    
    def __init__(self):
        # 초기화 순서 중요: Zookeeper → Kafka → DB
        self.initializers = [
            ZookeeperInitializer(),
            KafkaInitializer(),
            MongoInitializer(),
            QdrantInitializer()
        ]
    
    async def initialize_all(self) -> bool:
        """전체 시스템 초기화"""
        logger.info("🚀 Starting IACSRAG system initialization...")
        
        # Docker 확인
        if not DockerManager.is_docker_running():
            logger.error("Docker is not running. Please start Docker first.")
            return False
        
        # 각 서비스 초기화
        results = []
        for initializer in self.initializers:
            logger.info(f"\n{'='*50}")
            logger.info(f"Initializing {initializer.service_name}...")
            
            success = await initializer.setup()
            results.append((initializer.service_name, success))
            
            if not success:
                logger.error(f"Failed to initialize {initializer.service_name}")
                break
        
        # 결과 요약
        logger.info(f"\n{'='*50}")
        logger.info("Initialization Summary:")
        for service, success in results:
            status = "✅ Success" if success else "❌ Failed"
            logger.info(f"  {service}: {status}")
        
        all_success = all(success for _, success in results)
        
        if all_success:
            logger.info("\n✨ System initialization completed successfully!")
            logger.info("\n📊 Service URLs:")
            logger.info("  - MongoDB: mongodb://localhost:27017")
            logger.info("  - Qdrant: http://localhost:6333/dashboard")
            logger.info("  - Kafka-UI: http://localhost:8080")
        else:
            logger.error("\n❌ System initialization failed!")
        
        return all_success
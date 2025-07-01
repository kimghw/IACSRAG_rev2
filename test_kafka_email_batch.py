# test_kafka_email_batch.py
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

from aiokafka import AIOKafkaConsumer
from infra.core.config import settings
from content_email.email_batch_processor import EmailBatchProcessor
from content_email.orchestrator import EmailOrchestrator

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Kafka 관련 로그 레벨 조정
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)

class TestEmailBatchProcessor:
    """테스트용 이메일 배치 프로세서"""
    
    def __init__(self, batch_size: int = 3):
        self.batch_size = batch_size
        self.orchestrator = EmailOrchestrator()
        self.batch_processor = EmailBatchProcessor(
            batch_size=batch_size,
            orchestrator=self.orchestrator,
            max_wait_time=5.0  # 5초 대기
        )
        self.stats = {
            'total_events': 0,
            'total_batches': 0,
            'total_emails': 0
        }
    
    async def process_event(self, event_data: Dict[str, Any]):
        """이벤트를 배치 프로세서에 추가"""
        # 통계 업데이트
        self.stats['total_events'] += 1
        emails = event_data.get('response_data', {}).get('value', [])
        self.stats['total_emails'] += len(emails)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📥 Adding event to batch processor")
        logger.info(f"   - Event #{self.stats['total_events']}")
        logger.info(f"   - Event ID: {event_data.get('event_id')}")
        logger.info(f"   - Emails in event: {len(emails)}")
        logger.info(f"   - Current batch size: {len(self.batch_processor.current_batch)}/{self.batch_size}")
        
        # 배치 프로세서에 추가
        await self.batch_processor.add_event(event_data)
        
        # 배치가 처리되었는지 확인
        if len(self.batch_processor.current_batch) == 0:
            self.stats['total_batches'] += 1
            logger.info(f"✅ Batch #{self.stats['total_batches']} processed!")
    
    def get_stats(self):
        """현재 통계 반환"""
        return {
            **self.stats,
            'current_batch_size': len(self.batch_processor.current_batch),
            'batch_processor_stats': self.batch_processor.get_stats()
        }

async def test_kafka_email_batch():
    """Kafka에서 이메일 이벤트를 가져와서 배치 처리 테스트"""
    
    # 테스트용 배치 프로세서 생성
    test_processor = TestEmailBatchProcessor(batch_size=3)
    
    # Kafka Consumer 생성
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC_EMAIL_RECEIVED,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"test-batch-{datetime.now().timestamp()}",  # 유니크한 그룹 ID
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # 처음부터 읽기
        enable_auto_commit=False,  # 수동 커밋 (테스트용)
    )
    
    logger.info("🚀 Starting Email Batch Processing Test")
    logger.info(f"📡 Kafka Topic: {settings.KAFKA_TOPIC_EMAIL_RECEIVED}")
    logger.info(f"🔧 Batch Size: {test_processor.batch_size}")
    logger.info(f"⏱️  Max Wait Time: {test_processor.batch_processor.max_wait_time}s")
    
    await consumer.start()
    
    try:
        max_events = 10  # 최대 10개 이벤트 처리
        event_count = 0
        
        logger.info(f"\n📨 Fetching up to {max_events} events from Kafka...")
        logger.info("=" * 80)
        
        async for msg in consumer:
            event_count += 1
            
            # 이벤트 정보 출력
            event_data = msg.value
            event_id = event_data.get('event_id', 'unknown')
            account_id = event_data.get('account_id', 'unknown')
            emails = event_data.get('response_data', {}).get('value', [])
            
            logger.info(f"\n🔍 Event {event_count}/{max_events} received")
            logger.info(f"   - Event ID: {event_id}")
            logger.info(f"   - Account ID: {account_id}")
            logger.info(f"   - Timestamp: {event_data.get('occurred_at')}")
            logger.info(f"   - Email count: {len(emails)}")
            
            # 첫 번째 이메일 정보 샘플
            if emails:
                first_email = emails[0]
                logger.info(f"   - Sample email:")
                logger.info(f"     • Subject: {first_email.get('subject', 'No subject')[:50]}...")
                logger.info(f"     • From: {first_email.get('from_address', {}).get('emailAddress', {}).get('address', 'Unknown')}")
                logger.info(f"     • Has attachments: {first_email.get('has_attachments', False)}")
            
            # 배치 프로세서에 추가
            await test_processor.process_event(event_data)
            
            # 최대 이벤트 수에 도달하면 중단
            if event_count >= max_events:
                logger.info(f"\n🏁 Reached maximum events ({max_events}). Stopping...")
                break
        
        # 남은 이벤트 처리
        if len(test_processor.batch_processor.current_batch) > 0:
            logger.info(f"\n⏳ Flushing remaining {len(test_processor.batch_processor.current_batch)} events...")
            await test_processor.batch_processor.flush()
            test_processor.stats['total_batches'] += 1
        
        # 최종 통계
        stats = test_processor.get_stats()
        batch_stats = stats['batch_processor_stats']
        
        logger.info("\n" + "=" * 80)
        logger.info("📊 Test Summary:")
        logger.info(f"   - Total events processed: {stats['total_events']}")
        logger.info(f"   - Total emails processed: {stats['total_emails']}")
        logger.info(f"   - Total batches created: {stats['total_batches']}")
        logger.info(f"   - Batch processor stats:")
        logger.info(f"     • Batches processed: {batch_stats['total_batches_processed']}")
        logger.info(f"     • Emails processed: {batch_stats['total_emails_processed']}")
        logger.info(f"     • Failed batches: {batch_stats['failed_batches']}")
        logger.info(f"     • Average batch size: {batch_stats['average_batch_size']:.1f}")
        
        if event_count == 0:
            logger.warning("⚠️  No events found in Kafka topic!")
            
    except Exception as e:
        logger.error(f"❌ Error during test: {str(e)}", exc_info=True)
        
    finally:
        await consumer.stop()
        logger.info("\n🔌 Kafka consumer stopped")

async def test_with_timeout():
    """타임아웃 테스트 - 3개 미만의 이벤트가 있을 때"""
    logger.info("\n" + "=" * 80)
    logger.info("🕐 Testing batch timeout behavior...")
    logger.info("This test will wait for the timeout if less than 3 events are available")
    
    # 짧은 타임아웃으로 테스트
    test_processor = TestEmailBatchProcessor(batch_size=3)
    test_processor.batch_processor.max_wait_time = 3.0  # 3초로 단축
    
    # 더미 이벤트 2개 생성
    for i in range(2):
        dummy_event = {
            'event_id': f'test-timeout-{i}',
            'account_id': 'test-account',
            'occurred_at': datetime.now().isoformat(),
            'response_data': {
                'value': [{
                    'id': f'email-{i}',
                    'subject': f'Test Email {i}',
                    'from_address': {
                        'emailAddress': {
                            'address': f'test{i}@example.com'
                        }
                    },
                    'body': {
                        'content': f'Test content {i}'
                    },
                    'has_attachments': False,
                    'is_read': False,
                    'importance': 'normal'
                }]
            }
        }
        
        logger.info(f"\n📧 Adding test event {i+1}/2")
        await test_processor.process_event(dummy_event)
    
    logger.info("\n⏳ Waiting for timeout (3 seconds)...")
    await asyncio.sleep(4)  # 타임아웃보다 약간 더 대기
    
    stats = test_processor.get_stats()
    logger.info(f"\n📊 Timeout test results:")
    logger.info(f"   - Events added: 2")
    logger.info(f"   - Batches processed: {stats['total_batches']}")
    logger.info(f"   - Should be 1 batch processed after timeout")

async def main():
    """메인 함수"""
    logger.info("🎯 Email Batch Processing Test")
    logger.info(f"📍 Environment: {settings.KAFKA_CONSUMER_GROUP_ID}")
    logger.info(f"⚙️  Batch Size: {settings.EMAIL_BATCH_SIZE}")
    logger.info(f"⏰ Max Wait Time: {settings.EMAIL_BATCH_MAX_WAIT_TIME}s")
    
    # 선택할 테스트
    print("\n테스트 선택:")
    print("1. Kafka에서 실제 이벤트 배치 처리")
    print("2. 타임아웃 동작 테스트")
    print("3. 모든 테스트 실행")
    
    choice = input("\n선택 (1-3): ").strip()
    
    try:
        if choice == '1':
            await test_kafka_email_batch()
        elif choice == '2':
            await test_with_timeout()
        elif choice == '3':
            await test_kafka_email_batch()
            await test_with_timeout()
        else:
            logger.warning("잘못된 선택입니다.")
    except KeyboardInterrupt:
        logger.info("\n⚠️  Test interrupted by user")
    except Exception as e:
        logger.error(f"❌ Test failed: {str(e)}", exc_info=True)
    
    logger.info("\n✅ Test completed!")

if __name__ == "__main__":
    # .env 파일 로드
    from dotenv import load_dotenv
    import os
    
    env_file = '.env.development' if os.path.exists('.env.development') else '.env'
    load_dotenv(env_file)
    
    # 테스트 실행
    asyncio.run(main())

# test_kafka_email_events.py
import asyncio
import json
import logging
from datetime import datetime
from typing import List, Dict, Any

from aiokafka import AIOKafkaConsumer
from infra.core.config import settings
from infra.events.handlers.content_email_handler import handle_email_event

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def handle_batch_email_events(events: List[Dict[str, Any]]):
    """여러 이벤트를 하나의 배치로 처리"""
    batch_start = asyncio.get_event_loop().time()
    
    # 모든 이벤트에서 이메일 수집
    all_emails = []
    event_email_mapping = {}  # 어떤 이메일이 어떤 이벤트에서 왔는지 추적
    
    for event in events:
        event_id = event.get('event_id', 'unknown')
        response_data = event.get('response_data', {})
        emails = response_data.get('value', [])
        
        for email in emails:
            # 이메일에 원본 이벤트 정보 추가
            email['_event_id'] = event_id
            email['_account_id'] = event.get('account_id')
            all_emails.append(email)
        
        event_email_mapping[event_id] = len(emails)
    
    logger.info(f"📧 Processing batch of {len(events)} events with total {len(all_emails)} emails")
    
    # 배치 이벤트 구성
    batch_event = {
        'event_id': f"batch-{datetime.now().isoformat()}",
        'account_id': events[0].get('account_id'),  # 첫 번째 이벤트의 account_id 사용
        'occurred_at': datetime.now().isoformat(),
        'response_data': {
            'value': all_emails
        },
        '_batch_info': {
            'original_events': [e.get('event_id') for e in events],
            'email_count_per_event': event_email_mapping
        }
    }
    
    # 배치로 처리
    await handle_email_event(batch_event)
    
    processing_time = asyncio.get_event_loop().time() - batch_start
    logger.info(f"✅ Batch processing completed in {processing_time:.2f} seconds")
    logger.info(f"   - Events processed: {len(events)}")
    logger.info(f"   - Total emails: {len(all_emails)}")
    
    return batch_event

async def test_email_events_from_kafka():
    """Kafka에서 실제 이메일 이벤트들을 가져와서 배치로 테스트"""
    
    # Kafka Consumer 생성
    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC_EMAIL_RECEIVED,
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"test-email-{datetime.now().timestamp()}",  # 유니크한 그룹 ID
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # 처음부터 읽기
        enable_auto_commit=False,  # 수동 커밋 (테스트용)
    )
    
    logger.info(f"Connecting to Kafka topic: {settings.KAFKA_TOPIC_EMAIL_RECEIVED}")
    logger.info(f"Bootstrap servers: {settings.KAFKA_BOOTSTRAP_SERVERS}")
    
    await consumer.start()
    
    try:
        max_events = 3
        batch_size = 3  # 3개씩 배치로 처리
        
        logger.info(f"Fetching up to {max_events} events from Kafka for batch processing...")
        
        # 이벤트들을 수집
        collected_events = []
        event_count = 0
        
        async for msg in consumer:
            event_count += 1
            
            logger.info(f"\n{'='*80}")
            logger.info(f"Collecting Event {event_count}/{max_events}")
            logger.info(f"Topic: {msg.topic}")
            logger.info(f"Partition: {msg.partition}")
            logger.info(f"Offset: {msg.offset}")
            logger.info(f"Timestamp: {datetime.fromtimestamp(msg.timestamp/1000)}")
            
            event_data = msg.value
            event_id = event_data.get('event_id', 'unknown')
            account_id = event_data.get('account_id', 'unknown')
            
            logger.info(f"Event ID: {event_id}")
            logger.info(f"Account ID: {account_id}")
            
            # 이메일 정보 출력
            response_data = event_data.get('response_data', {})
            emails = response_data.get('value', [])
            logger.info(f"Number of emails in this event: {len(emails)}")
            
            if emails:
                first_email = emails[0]
                logger.info(f"First email:")
                logger.info(f"  - Subject: {first_email.get('subject', 'No subject')}")
                logger.info(f"  - From: {first_email.get('from_address', {}).get('emailAddress', {}).get('address', 'Unknown')}")
                logger.info(f"  - Has attachments: {first_email.get('has_attachments', False)}")
            
            # 이벤트 수집
            collected_events.append(event_data)
            
            # 배치 크기에 도달하거나 마지막 이벤트인 경우 처리
            if len(collected_events) >= batch_size or event_count >= max_events:
                logger.info(f"\n{'='*40} BATCH PROCESSING {'='*40}")
                logger.info(f"Processing batch of {len(collected_events)} events as single batch")
                
                try:
                    # 배치 처리
                    await handle_batch_email_events(collected_events)
                except Exception as e:
                    logger.error(f"❌ Batch processing failed: {str(e)}", exc_info=True)
                
                # 배치 처리 완료 후 초기화
                collected_events = []
                
                if event_count >= max_events:
                    logger.info(f"\nProcessed {max_events} events. Stopping.")
                    break
        
        if event_count == 0:
            logger.warning("No events found in Kafka topic!")
        else:
            logger.info(f"\nTotal events processed: {event_count}")
            
    except Exception as e:
        logger.error(f"Error during test: {str(e)}", exc_info=True)
        
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped")

async def main():
    """메인 함수"""
    logger.info("Starting Email Event Test from Kafka")
    logger.info(f"Environment: {settings.KAFKA_CONSUMER_GROUP_ID}")
    
    try:
        await test_email_events_from_kafka()
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {str(e)}", exc_info=True)
    
    logger.info("\nTest completed!")

if __name__ == "__main__":
    # 테스트 실행
    asyncio.run(main())
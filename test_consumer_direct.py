#!/usr/bin/env python3
"""PDF Consumer 직접 테스트"""
import asyncio
import logging
import json
from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 라이브러리 로그 레벨 조정
logging.getLogger("aiokafka").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.WARNING)

async def test_direct_consumer():
    """Consumer를 직접 생성하여 메시지 수신 테스트"""
    consumer = None
    
    try:
        logger.info("Creating direct consumer...")
        
        consumer = AIOKafkaConsumer(
            'document.uploaded',
            bootstrap_servers='localhost:9092',
            group_id='test-direct-consumer',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        
        logger.info("Starting consumer...")
        await consumer.start()
        logger.info("✅ Consumer started successfully")
        
        # 구독 정보 확인
        subscriptions = consumer.subscription()
        logger.info(f"Subscriptions: {subscriptions}")
        
        # 파티션 할당 확인
        await asyncio.sleep(2)  # 파티션 할당 대기
        assignments = consumer.assignment()
        logger.info(f"Assigned partitions: {assignments}")
        
        # 메시지 수신 대기
        logger.info("\n📨 Waiting for messages (10 seconds)...")
        
        message_count = 0
        timeout = asyncio.create_task(asyncio.sleep(10))
        
        while not timeout.done():
            try:
                # 1초 타임아웃으로 메시지 대기
                msg_batch = await asyncio.wait_for(
                    consumer.getmany(timeout_ms=1000),
                    timeout=1.5
                )
                
                for tp, messages in msg_batch.items():
                    for msg in messages:
                        message_count += 1
                        value = json.loads(msg.value.decode())
                        logger.info(f"\n✅ Message received:")
                        logger.info(f"   Topic: {msg.topic}")
                        logger.info(f"   Partition: {msg.partition}")
                        logger.info(f"   Offset: {msg.offset}")
                        logger.info(f"   Document ID: {value.get('document_id')}")
                        logger.info(f"   Content Type: {value.get('content_type')}")
                        
            except asyncio.TimeoutError:
                # 타임아웃은 정상 - 계속 대기
                pass
        
        logger.info(f"\n📊 Total messages received: {message_count}")
        
    except Exception as e:
        logger.error(f"❌ Consumer test failed: {e}", exc_info=True)
        
    finally:
        if consumer:
            await consumer.stop()
            logger.info("Consumer stopped")

async def test_pdf_consumer_class():
    """PdfConsumer 클래스 직접 테스트"""
    from content_pdf.pdf_consumer import PdfConsumer
    
    logger.info("\n🧪 Testing PdfConsumer class directly...")
    
    try:
        pdf_consumer = PdfConsumer()
        logger.info("✅ PdfConsumer instance created")
        
        # 속성 확인
        logger.info(f"Topics: {pdf_consumer.topics}")
        logger.info(f"Group ID: {pdf_consumer.group_id}")
        
        # 짧은 시간동안 실행
        consumer_task = asyncio.create_task(pdf_consumer.start())
        await asyncio.sleep(5)
        
        logger.info("Stopping PdfConsumer...")
        await pdf_consumer.stop()
        
        # Task 완료 대기
        try:
            await asyncio.wait_for(consumer_task, timeout=2)
        except asyncio.TimeoutError:
            logger.warning("Consumer did not stop in time")
            consumer_task.cancel()
            
    except Exception as e:
        logger.error(f"PdfConsumer test failed: {e}", exc_info=True)

if __name__ == "__main__":
    logger.info("🔍 Consumer Direct Test\n")
    
    # 1. 직접 Consumer 테스트
    asyncio.run(test_direct_consumer())
    
    # 2. PdfConsumer 클래스 테스트
    asyncio.run(test_pdf_consumer_class())
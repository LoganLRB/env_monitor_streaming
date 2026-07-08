import asyncio
import logging

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from streaming import alert, consumer, state
from streaming.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    logger.info("Starting env_monitor_streaming (environment=%s)", settings.environment)

    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers)
    alert_consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="alert-consumer",
        auto_offset_reset="earliest",
    )
    state_consumer = AIOKafkaConsumer(
        settings.kafka_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="state-consumer",
        auto_offset_reset="earliest",
    )
    redis = aioredis.from_url(settings.redis_url, decode_responses=False)

    await producer.start()
    await alert_consumer.start()
    await state_consumer.start()
    logger.info(
        "Connected — Kafka: %s  Redis: %s",
        settings.kafka_bootstrap_servers,
        settings.redis_url,
    )

    try:
        await asyncio.gather(
            consumer.run(producer),
            alert.run(alert_consumer, redis),
            state.run(state_consumer, redis),
        )
    finally:
        await producer.stop()
        await alert_consumer.stop()
        await state_consumer.stop()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())

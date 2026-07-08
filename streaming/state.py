"""State consumer: writes current sensor state and 15-minute rolling history to Redis."""
import logging

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from streaming.models import SensorReading

logger = logging.getLogger(__name__)

_CURRENT_TTL = 600    # 10 minutes — key expires if sensor goes silent
_WINDOW_SECONDS = 900  # 15-minute rolling history window


def _current_key(sensor_id: str) -> str:
    return f"sensor:current:{sensor_id}"


def _history_key(sensor_id: str) -> str:
    return f"sensor:history:{sensor_id}"


async def run(consumer: AIOKafkaConsumer, redis: aioredis.Redis) -> None:
    async for msg in consumer:
        try:
            reading = SensorReading.model_validate_json(msg.value)
            await _write(reading, msg.value, redis)
        except Exception as exc:
            logger.warning("State consumer error: %s", exc)


async def _write(reading: SensorReading, raw: bytes, redis: aioredis.Redis) -> None:
    ts = reading.timestamp.timestamp()

    pipe = redis.pipeline()
    pipe.set(_current_key(reading.sensor_id), raw, ex=_CURRENT_TTL)
    pipe.zadd(_history_key(reading.sensor_id), {raw: ts})
    pipe.zremrangebyscore(_history_key(reading.sensor_id), "-inf", ts - _WINDOW_SECONDS)
    pipe.publish("sensor.live", raw)
    await pipe.execute()

    logger.debug("State written: %s", reading.sensor_id)

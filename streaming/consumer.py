"""SSE consumer: reads sensor_reading events from env_monitor_api and publishes to Kafka."""
import asyncio
import logging

import httpx
from aiokafka import AIOKafkaProducer

from streaming.config import settings
from streaming.models import SensorReading

logger = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 60.0


async def run(producer: AIOKafkaProducer) -> None:
    url = f"{settings.api_base_url}/v1/sensors/stream"
    backoff = _INITIAL_BACKOFF
    while True:
        try:
            await _stream(url, producer)
            backoff = _INITIAL_BACKOFF
        except Exception as exc:
            logger.warning("SSE stream lost: %s; reconnecting in %.0fs", exc, backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)


async def _stream(url: str, producer: AIOKafkaProducer) -> None:
    async with httpx.AsyncClient(timeout=None) as client:
        async with client.stream("GET", url) as response:
            response.raise_for_status()
            current_event: str | None = None
            async for line in response.aiter_lines():
                if line.startswith("event:"):
                    current_event = line[6:].strip()
                elif line.startswith("data:") and current_event == "sensor_reading":
                    raw = line[5:].strip()
                    try:
                        reading = SensorReading.model_validate_json(raw)
                        await producer.send_and_wait(settings.kafka_topic, raw.encode())
                        logger.debug("Published: %s %s", reading.sensor_id, reading.wildfire_risk)
                    except Exception as exc:
                        logger.warning("Skipped malformed event: %s", exc)
                    current_event = None
                elif line == "":
                    current_event = None

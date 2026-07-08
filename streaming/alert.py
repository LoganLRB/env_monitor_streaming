"""Alert engine: tracks severity per sensor and fires SNS alerts on escalation."""
import json
import logging

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

from streaming.config import settings
from streaming.models import SensorReading

logger = logging.getLogger(__name__)

SEVERITY_ORDER: dict[str, int] = {"LOW": 0, "MODERATE": 1, "HIGH": 2, "CRITICAL": 3}


def _severity_key(sensor_id: str) -> str:
    return f"sensor:severity:{sensor_id}"


async def run(consumer: AIOKafkaConsumer, redis: aioredis.Redis) -> None:
    async for msg in consumer:
        try:
            reading = SensorReading.model_validate_json(msg.value)
            await _evaluate(reading, redis)
        except Exception as exc:
            logger.warning("Alert consumer error: %s", exc)


async def _evaluate(reading: SensorReading, redis: aioredis.Redis) -> None:
    new = reading.wildfire_risk
    stored = await redis.get(_severity_key(reading.sensor_id))
    current = stored.decode() if stored else "LOW"

    if SEVERITY_ORDER[new] > SEVERITY_ORDER[current]:
        await _fire_alert(reading, previous=current)

    await redis.set(_severity_key(reading.sensor_id), new)


async def _fire_alert(reading: SensorReading, previous: str) -> None:
    payload = {
        "sensor_id": reading.sensor_id,
        "zone_id": reading.zone_id,
        "zone_name": reading.zone_name,
        "severity": reading.wildfire_risk,
        "previous_severity": previous,
        "timestamp": reading.timestamp.isoformat(),
        "temperature_f": reading.temperature_f,
        "humidity_pct": reading.humidity_pct,
        "wind_speed_mph": reading.wind_speed_mph,
        "pm25_ugm3": reading.pm25_ugm3,
        "latitude": reading.latitude,
        "longitude": reading.longitude,
    }

    if settings.is_local:
        logger.info(
            "[ALERT] %s → %s | %s (%s) | temp=%.1f°F hum=%.1f%% wind=%.1fmph pm25=%.1f",
            previous, reading.wildfire_risk, reading.sensor_id, reading.zone_name,
            reading.temperature_f, reading.humidity_pct,
            reading.wind_speed_mph, reading.pm25_ugm3,
        )
        return

    import boto3
    sns = boto3.client("sns")
    sns.publish(
        TopicArn=settings.sns_alert_topic_arn,
        Subject=f"[{reading.wildfire_risk}] Wildfire risk escalation: {reading.zone_name}",
        Message=json.dumps(payload),
    )
    logger.info("SNS alert published: %s %s → %s", reading.sensor_id, previous, reading.wildfire_risk)

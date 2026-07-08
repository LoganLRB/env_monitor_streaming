import json
import unittest
from datetime import datetime, timedelta, timezone

import fakeredis.aioredis

from streaming.models import SensorReading
from streaming.state import _current_key, _history_key, _write


def _reading(sensor_id="SNS-001", ts=None) -> tuple[SensorReading, bytes]:
    ts = ts or datetime.now(timezone.utc)
    r = SensorReading(
        sensor_id=sensor_id,
        zone_id="zone-a",
        zone_name="Northern Forest Ridge",
        timestamp=ts,
        temperature_f=85.0,
        humidity_pct=45.0,
        wind_speed_mph=5.0,
        pm25_ugm3=10.0,
        battery_pct=95.0,
        latitude=37.5123,
        longitude=-119.5341,
        wildfire_risk="LOW",
    )
    return r, r.model_dump_json().encode()


class TestStateConsumer(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.redis = fakeredis.aioredis.FakeRedis()

    async def asyncTearDown(self):
        await self.redis.aclose()

    async def test_current_key_written(self):
        reading, raw = _reading()
        await _write(reading, raw, self.redis)
        stored = await self.redis.get(_current_key("SNS-001"))
        self.assertIsNotNone(stored)
        self.assertEqual(json.loads(stored)["sensor_id"], "SNS-001")

    async def test_current_key_has_ttl(self):
        reading, raw = _reading()
        await _write(reading, raw, self.redis)
        ttl = await self.redis.ttl(_current_key("SNS-001"))
        self.assertGreater(ttl, 0)

    async def test_reading_added_to_history(self):
        reading, raw = _reading()
        await _write(reading, raw, self.redis)
        count = await self.redis.zcard(_history_key("SNS-001"))
        self.assertEqual(count, 1)

    async def test_old_reading_pruned_from_history(self):
        now = datetime.now(timezone.utc)
        old, old_raw = _reading(ts=now - timedelta(minutes=20))
        await _write(old, old_raw, self.redis)

        current, current_raw = _reading(ts=now)
        await _write(current, current_raw, self.redis)

        count = await self.redis.zcard(_history_key("SNS-001"))
        self.assertEqual(count, 1)  # old reading pruned

    async def test_readings_within_window_retained(self):
        now = datetime.now(timezone.utc)
        for minutes_ago in [1, 5, 10, 14]:
            r, raw = _reading(ts=now - timedelta(minutes=minutes_ago))
            await _write(r, raw, self.redis)
        count = await self.redis.zcard(_history_key("SNS-001"))
        self.assertEqual(count, 4)

    async def test_multiple_sensors_independent(self):
        r1, raw1 = _reading(sensor_id="SNS-001")
        r2, raw2 = _reading(sensor_id="SNS-002")
        await _write(r1, raw1, self.redis)
        await _write(r2, raw2, self.redis)

        s1 = json.loads(await self.redis.get(_current_key("SNS-001")))
        s2 = json.loads(await self.redis.get(_current_key("SNS-002")))
        self.assertEqual(s1["sensor_id"], "SNS-001")
        self.assertEqual(s2["sensor_id"], "SNS-002")

    async def test_pubsub_message_published(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("sensor.live")
        await pubsub.get_message(ignore_subscribe_messages=True)

        reading, raw = _reading()
        await _write(reading, raw, self.redis)

        msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        self.assertIsNotNone(msg)
        self.assertEqual(json.loads(msg["data"])["sensor_id"], "SNS-001")
        await pubsub.unsubscribe()

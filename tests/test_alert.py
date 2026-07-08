import unittest
from datetime import datetime, timezone
from unittest.mock import AsyncMock, patch

import fakeredis.aioredis

from streaming.alert import _evaluate, _severity_key
from streaming.models import SensorReading


def _reading(sensor_id="SNS-001", wildfire_risk="LOW") -> SensorReading:
    return SensorReading(
        sensor_id=sensor_id,
        zone_id="zone-a",
        zone_name="Northern Forest Ridge",
        timestamp=datetime.now(timezone.utc),
        temperature_f=85.0,
        humidity_pct=45.0,
        wind_speed_mph=5.0,
        pm25_ugm3=10.0,
        battery_pct=95.0,
        latitude=37.5123,
        longitude=-119.5341,
        wildfire_risk=wildfire_risk,
    )


class TestAlertEngine(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        self.redis = fakeredis.aioredis.FakeRedis()

    async def asyncTearDown(self):
        await self.redis.aclose()

    async def test_low_to_moderate_fires_alert(self):
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="MODERATE"), self.redis)
        mock_fire.assert_awaited_once()
        _, kwargs = mock_fire.call_args
        self.assertEqual(kwargs["previous"], "LOW")

    async def test_moderate_to_high_fires_alert(self):
        await self.redis.set(_severity_key("SNS-001"), "MODERATE")
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="HIGH"), self.redis)
        mock_fire.assert_awaited_once()

    async def test_high_to_critical_fires_alert(self):
        await self.redis.set(_severity_key("SNS-001"), "HIGH")
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="CRITICAL"), self.redis)
        mock_fire.assert_awaited_once()

    async def test_same_severity_no_alert(self):
        await self.redis.set(_severity_key("SNS-001"), "HIGH")
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="HIGH"), self.redis)
        mock_fire.assert_not_awaited()

    async def test_de_escalation_no_alert(self):
        await self.redis.set(_severity_key("SNS-001"), "CRITICAL")
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="HIGH"), self.redis)
        mock_fire.assert_not_awaited()

    async def test_re_escalation_after_de_escalation_fires_alert(self):
        await self.redis.set(_severity_key("SNS-001"), "CRITICAL")
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock):
            await _evaluate(_reading(wildfire_risk="LOW"), self.redis)
        # Stored is now LOW; escalating to HIGH should alert
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="HIGH"), self.redis)
        mock_fire.assert_awaited_once()

    async def test_no_prior_state_defaults_to_low(self):
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="MODERATE"), self.redis)
        mock_fire.assert_awaited_once()

    async def test_severity_persisted_after_evaluation(self):
        await _evaluate(_reading(wildfire_risk="HIGH"), self.redis)
        stored = await self.redis.get(_severity_key("SNS-001"))
        self.assertEqual(stored.decode(), "HIGH")

    async def test_low_reading_no_alert(self):
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(wildfire_risk="LOW"), self.redis)
        mock_fire.assert_not_awaited()

    async def test_multiple_sensors_independent(self):
        with patch("streaming.alert._fire_alert", new_callable=AsyncMock) as mock_fire:
            await _evaluate(_reading(sensor_id="SNS-001", wildfire_risk="HIGH"), self.redis)
            await _evaluate(_reading(sensor_id="SNS-002", wildfire_risk="LOW"), self.redis)
        self.assertEqual(mock_fire.await_count, 1)
        s1 = await self.redis.get(_severity_key("SNS-001"))
        s2 = await self.redis.get(_severity_key("SNS-002"))
        self.assertEqual(s1.decode(), "HIGH")
        self.assertEqual(s2.decode(), "LOW")

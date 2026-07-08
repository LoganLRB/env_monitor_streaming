from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class SensorReading(BaseModel):
    sensor_id: str
    zone_id: str
    zone_name: str
    timestamp: datetime
    temperature_f: float
    humidity_pct: float
    wind_speed_mph: float
    pm25_ugm3: float
    battery_pct: float
    latitude: float
    longitude: float
    wildfire_risk: Literal["LOW", "MODERATE", "HIGH", "CRITICAL"]

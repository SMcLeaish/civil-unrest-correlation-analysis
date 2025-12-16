from typing import Any
from pydantic import BaseModel

class CountryMeta(BaseModel):
    iso: str
    name: str


class AcledEvent(BaseModel):
    event_date: str
    admin1: str | None = None
    location: str | None = None
    event_type: str | None = None
    sub_event_type: str | None = None
    fatalities: int | None = None
    notes: str | None = None


class OecdMetric(BaseModel):
    feature: str
    value: float


class SnapshotResponse(BaseModel):
    iso: str
    country: str
    start: str
    end: str
    #oecd_metrics: list[OecdMetric]
    acled_events: list[AcledEvent]
    #map_spec: dict[str, Any]

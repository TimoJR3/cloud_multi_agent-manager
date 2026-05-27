from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


class EventEnvelope(BaseModel):
    event_type: str
    schema_version: str = "1"
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source: str
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    ttl_seconds: int | None = Field(default=None, gt=0)
    expires_at: datetime | None = None
    payload: dict[str, Any]

    def model_post_init(self, __context: Any) -> None:
        if self.ttl_seconds is not None and self.expires_at is None:
            self.expires_at = self.occurred_at + timedelta(seconds=self.ttl_seconds)

    def is_expired(self, now: datetime | None = None) -> bool:
        if self.expires_at is None:
            return False
        current = now or datetime.now(UTC)
        expires_at = self.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        return current >= expires_at

    def broker_payload(self) -> dict[str, Any]:
        payload = self.model_dump(mode="json")
        payload["message_id"] = str(uuid4())
        return payload


class TaskCreate(BaseModel):
    task_type: str = "generic"
    cpu_required: float = Field(default=1.0, gt=0)
    ram_required_mb: int = Field(default=512, gt=0)
    duration_seconds: float = Field(default=5.0, gt=0)
    priority: int = Field(default=1, ge=0, le=10)
    sla_deadline_seconds: int = Field(default=30, gt=0)
    requires_gpu: bool = False
    payload: dict[str, Any] = Field(default_factory=dict)


class TaskView(BaseModel):
    task_id: str
    status: str
    task_type: str
    priority: int
    cpu_required: float
    ram_required_mb: int
    requires_gpu: bool
    created_at: datetime
    updated_at: datetime
    assigned_node_id: str | None = None
    error: str | None = None


class HealthComponent(BaseModel):
    name: str
    status: Literal["ok", "degraded", "down"]
    details: dict[str, Any] = Field(default_factory=dict)


class HealthResponse(BaseModel):
    service: str
    status: Literal["ok", "degraded", "down"]
    components: list[HealthComponent]

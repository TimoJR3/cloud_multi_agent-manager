import asyncio

import pytest

from cloudrm.messages import EventEnvelope
from services.coordinator_agent import main as coordinator


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = {}

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value: str, ex: int | None = None):
        self.values[key] = value

    async def hset(self, key: str, mapping: dict[str, str]):
        self.hashes[key] = mapping


def test_coordinator_selects_best_utility() -> None:
    coordinator.runtime.config = {
        "agents": {
            "coordinator": {
                "weights": {"fit": 0.5, "sla": 0.2, "balance": 0.2, "forecast": 0.1, "cost": 0.1}
            }
        }
    }
    low = {"fit_score": 0.4, "balance_score": 0.5, "cost": 0.1}
    high = {"fit_score": 0.9, "balance_score": 0.8, "cost": 0.2}

    assert coordinator.utility(high, {"sla_risk": 0.2}, {"overload_risk": 0.1}) > coordinator.utility(
        low, {"sla_risk": 0.2}, {"overload_risk": 0.1}
    )


@pytest.mark.asyncio
async def test_coordinator_waits_for_decision_window(monkeypatch) -> None:
    calls: list[str] = []
    coordinator.runtime.config = {"agents": {"coordinator": {"decision_window_ms": 20}}}

    async def fake_decide(task_id: str, correlation_id: str) -> None:
        calls.append(f"{task_id}:{correlation_id}")

    monkeypatch.setattr(coordinator, "decide", fake_decide)
    await coordinator.schedule_decision("t1", "c1")
    await coordinator.schedule_decision("t1", "c1")

    assert calls == []
    await asyncio.sleep(0.05)
    assert calls == ["t1:c1"]


@pytest.mark.asyncio
async def test_high_sla_risk_with_safe_node_dispatches_and_advisory_scales(monkeypatch) -> None:
    published: list[EventEnvelope] = []
    redis = FakeRedis()
    coordinator.runtime.redis = redis
    coordinator.runtime.db = object()
    coordinator.runtime.config = {
        "agents": {
            "coordinator": {
                "weights": {"fit": 0.5, "sla": 0.2, "balance": 0.2, "forecast": 0.1, "cost": 0.1}
            }
        }
    }

    async def fake_publish(topic_key: str, envelope: EventEnvelope) -> None:
        published.append(envelope)

    async def fake_save_decision(*args, **kwargs) -> None:
        return None

    async def fake_update_task(*args, **kwargs) -> None:
        return None

    async def fake_load_payload(prefix: str, task_id: str):
        if prefix == "classified":
            return {"task_id": task_id, "cpu_required": 1, "ram_required_mb": 512, "classified_at_ts": 1}
        if prefix == "sla":
            return {"task_id": task_id, "sla_risk": 0.96, "sla_severity": "critical", "unsafe": True, "recommendation": "scale"}
        return None

    async def fake_load_proposals(task_id: str):
        return [{"task_id": task_id, "node_id": "node-a", "fit_score": 0.9, "balance_score": 0.8, "cost": 0.1, "unsafe": True}]

    monkeypatch.setattr(coordinator.runtime, "publish", fake_publish)
    monkeypatch.setattr(coordinator, "save_decision", fake_save_decision)
    monkeypatch.setattr(coordinator, "update_task", fake_update_task)
    monkeypatch.setattr(coordinator, "load_payload", fake_load_payload)
    monkeypatch.setattr(coordinator, "load_proposals", fake_load_proposals)

    await coordinator.decide("t1", "c1")

    event_types = [event.event_type for event in published]
    assert "decision.dispatch" in event_types
    assert "decision.scale" in event_types
    assert redis.values["decision:made:t1"] == "dispatch"


@pytest.mark.asyncio
async def test_no_safe_node_emits_scale_only_and_does_not_finalize_marker(monkeypatch) -> None:
    published: list[EventEnvelope] = []
    redis = FakeRedis()
    coordinator.runtime.redis = redis
    coordinator.runtime.db = object()
    coordinator.runtime.config = {}

    async def fake_publish(topic_key: str, envelope: EventEnvelope) -> None:
        published.append(envelope)

    async def fake_save_decision(*args, **kwargs) -> None:
        return None

    async def fake_load_payload(prefix: str, task_id: str):
        if prefix == "classified":
            return {"task_id": task_id, "cpu_required": 999, "ram_required_mb": 999999, "classified_at_ts": 1}
        return None

    async def fake_load_proposals(task_id: str):
        return [{"task_id": task_id, "node_id": None, "unsafe": True, "fit_score": 0.0}]

    monkeypatch.setattr(coordinator.runtime, "publish", fake_publish)
    monkeypatch.setattr(coordinator, "save_decision", fake_save_decision)
    monkeypatch.setattr(coordinator, "load_payload", fake_load_payload)
    monkeypatch.setattr(coordinator, "load_proposals", fake_load_proposals)

    await coordinator.decide("t2", "c2")

    assert [event.event_type for event in published] == ["decision.scale"]
    assert published[0].payload["requeue_after_scale"] is True
    assert await redis.get("decision:made:t2") is None

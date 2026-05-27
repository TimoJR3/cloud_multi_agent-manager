import asyncio

import pytest

from services.coordinator_agent import main as coordinator


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

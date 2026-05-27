from datetime import UTC, datetime, timedelta

from services.sla_agent.main import actual_sla_violation, assess_risk, risk_severity


def test_sla_risk_severity_levels() -> None:
    assert risk_severity(0.1) == "low"
    assert risk_severity(0.5) == "medium"
    assert risk_severity(0.8) == "high"
    assert risk_severity(0.95) == "critical"


def test_sla_risk_payload_contains_required_fields() -> None:
    assessment = assess_risk({"queue_length": 10, "duration_seconds": 2, "sla_deadline_seconds": 10})

    assert set(["estimated_wait", "deadline", "risk", "severity", "recommendation"]).issubset(assessment)
    assert assessment["severity"] in {"low", "medium", "high", "critical"}


def test_risk_is_not_actual_violation() -> None:
    now = datetime.now(UTC)
    violation = actual_sla_violation(
        {
            "created_at": now.isoformat(),
            "started_at": (now + timedelta(seconds=1)).isoformat(),
            "finished_at": (now + timedelta(seconds=2)).isoformat(),
            "sla_deadline_seconds": 10,
        },
        now=now + timedelta(seconds=2),
    )

    assert violation is None


def test_actual_violation_requires_exceeded_deadline() -> None:
    now = datetime.now(UTC)
    violation = actual_sla_violation(
        {
            "created_at": now.isoformat(),
            "started_at": (now + timedelta(seconds=12)).isoformat(),
            "sla_deadline_seconds": 10,
        },
        now=now + timedelta(seconds=12),
    )

    assert violation is not None
    assert violation["wait_seconds"] > violation["deadline"]

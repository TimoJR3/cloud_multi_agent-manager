from datetime import UTC, datetime, timedelta

from cloudrm.messages import EventEnvelope


def test_event_envelope_has_schema_version_and_ttl() -> None:
    envelope = EventEnvelope(event_type="request.created", source="test", ttl_seconds=30, payload={"task_id": "t1"})

    assert envelope.schema_version == "1"
    assert envelope.correlation_id
    assert envelope.event_id
    assert envelope.expires_at is not None
    assert envelope.expires_at > envelope.occurred_at


def test_broker_payload_has_unique_message_id() -> None:
    envelope = EventEnvelope(event_type="request.created", source="test", payload={"task_id": "t1"})

    first = envelope.broker_payload()
    second = envelope.broker_payload()

    assert first["correlation_id"] == second["correlation_id"] == envelope.correlation_id
    assert first["event_id"] == second["event_id"] == envelope.event_id
    assert first["message_id"] != second["message_id"]


def test_expired_event_is_detected() -> None:
    envelope = EventEnvelope(
        event_type="request.created",
        source="test",
        expires_at=datetime.now(UTC) - timedelta(seconds=1),
        payload={},
    )

    assert envelope.is_expired()

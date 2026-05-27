import pytest

import cloudrm.runtime as runtime_module
from cloudrm.messages import EventEnvelope
from cloudrm.runtime import ServiceRuntime


class FakeKafkaBus:
    def __init__(self) -> None:
        self.messages: list[tuple[str, EventEnvelope]] = []

    async def publish(self, topic: str, envelope: EventEnvelope) -> None:
        self.messages.append((topic, envelope))


class FakeRabbitPublisher:
    def __init__(self) -> None:
        self.messages: list[tuple[str, EventEnvelope]] = []

    async def publish(self, routing_key: str, envelope: EventEnvelope) -> str:
        self.messages.append((routing_key, envelope))
        return envelope.broker_payload()["message_id"]


@pytest.mark.asyncio
async def test_runtime_publish_sends_to_kafka_and_rabbitmq(monkeypatch) -> None:
    async def fake_record_event(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(runtime_module, "record_event", fake_record_event)
    service = ServiceRuntime("test-service")
    service.config = {"kafka": {"topics": {"request_created": "request.created"}}}
    service.db = object()
    service.bus = FakeKafkaBus()
    service.rabbit = FakeRabbitPublisher()

    envelope = EventEnvelope(event_type="request.created", source="test", payload={"task_id": "t1"})
    await service.publish("request_created", envelope)

    assert service.bus.messages[0][0] == "request.created"
    assert service.rabbit.messages[0][0] == "request.created"
    assert service.bus.messages[0][1].correlation_id == service.rabbit.messages[0][1].correlation_id

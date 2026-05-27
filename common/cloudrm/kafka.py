from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from cloudrm.messages import EventEnvelope
from cloudrm.metrics import CONSUMED_MESSAGES


class EventBus:
    def __init__(self, bootstrap_servers: str, service_name: str) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.service_name = service_name
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
        )
        await self.producer.start()

    async def stop(self) -> None:
        if self.producer is not None:
            await self.producer.stop()

    async def publish(self, topic: str, envelope: EventEnvelope) -> None:
        if self.producer is None:
            raise RuntimeError("Kafka producer не запущен")
        await self.producer.send_and_wait(topic, envelope.broker_payload(), key=envelope.correlation_id)


class EventConsumer:
    def __init__(self, bootstrap_servers: str, group_id: str, topics: list[str]) -> None:
        self.consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
            auto_offset_reset="earliest",
        )

    async def start(self) -> None:
        await self.consumer.start()

    async def stop(self) -> None:
        await self.consumer.stop()

    async def events(self) -> AsyncIterator[tuple[Any, EventEnvelope]]:
        async for message in self.consumer:
            envelope = EventEnvelope.model_validate(message.value)
            if envelope.is_expired():
                await self.consumer.commit()
                continue
            CONSUMED_MESSAGES.labels("kafka", message.topic, envelope.event_type, envelope.source).inc()
            yield message, envelope

    async def commit(self) -> None:
        await self.consumer.commit()


async def ping_kafka(bootstrap_servers: str) -> bool:
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers)
    await producer.start()
    await producer.stop()
    return True

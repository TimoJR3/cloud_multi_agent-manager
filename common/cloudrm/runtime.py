from __future__ import annotations

import logging
import os
from typing import Any, Protocol

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from cloudrm.cache import create_redis, ping_redis
from cloudrm.config import get_nested, load_config
from cloudrm.db import create_engine, ping_db, run_migrations
from cloudrm.kafka import EventBus, EventConsumer, ping_kafka
from cloudrm.messages import EventEnvelope, HealthComponent, HealthResponse
from cloudrm.metrics import KAFKA_EVENTS, RABBITMQ_EVENTS
from cloudrm.persistence import record_event
from cloudrm.rabbit import RabbitConsumer, RabbitMQSettings, RabbitPublisher, ping_rabbitmq
from cloudrm.retry import retry_async


class RuntimeConsumer(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def events(self) -> Any: ...
    async def commit(self) -> None: ...


class ServiceRuntime:
    def __init__(self, service_name: str, topics: list[str] | None = None) -> None:
        self.service_name = service_name
        self.topics = topics or []
        self.config: dict[str, Any] = {}
        self.db: AsyncEngine | None = None
        self.redis: Redis | None = None
        self.bus: EventBus | None = None
        self.consumer: RuntimeConsumer | None = None
        self.rabbit_settings: RabbitMQSettings | None = None
        self.rabbit: RabbitPublisher | None = None
        self.rabbit_consumer: RabbitConsumer | None = None
        self.event_backend = "rabbitmq"
        self.ready = False
        self.last_error: str | None = None
        self.logger = logging.getLogger(service_name)

    async def start(self) -> None:
        self.config = load_config()
        retry = get_nested(self.config, "system", "retry", default={})
        attempts = int(retry.get("attempts", 30))
        delay = float(retry.get("delay_seconds", 2))
        postgres_dsn = get_nested(self.config, "postgres", "dsn")
        redis_url = get_nested(self.config, "redis", "url")
        kafka_servers = get_nested(self.config, "kafka", "bootstrap_servers")
        self.rabbit_settings = RabbitMQSettings.from_config(self.config)
        self.event_backend = self._event_backend()

        self.logger.info("Starting service dependencies", extra={"cloudrm_backend": self.event_backend})
        self.db = create_engine(postgres_dsn)
        await retry_async(lambda: ping_db(self.db), attempts=attempts, delay_seconds=delay, name="postgres")
        await run_migrations(self.db)

        self.redis = create_redis(redis_url)
        await retry_async(lambda: ping_redis(self.redis), attempts=attempts, delay_seconds=delay, name="redis")

        if self.event_backend in {"kafka", "dual"}:
            await retry_async(lambda: ping_kafka(kafka_servers), attempts=attempts, delay_seconds=delay, name="kafka")
            self.bus = EventBus(kafka_servers, self.service_name)
            await self.bus.start()
            if self.topics and self.event_backend == "kafka":
                kafka_consumer = EventConsumer(kafka_servers, self.service_name, self.topics)
                await kafka_consumer.start()
                self.consumer = kafka_consumer

        if self.event_backend in {"rabbitmq", "dual"}:
            await retry_async(lambda: ping_rabbitmq(self.rabbit_settings), attempts=attempts, delay_seconds=delay, name="rabbitmq")
            self.rabbit = RabbitPublisher(self.rabbit_settings, self.service_name)
            await self.rabbit.start(attempts=attempts, delay_seconds=delay)
            queue_name = self.rabbit_settings.queue_for_service(self.service_name)
            if queue_name is not None:
                self.rabbit_consumer = RabbitConsumer(self.rabbit_settings, queue_name, self.service_name)
                await self.rabbit_consumer.start(attempts=attempts, delay_seconds=delay)
                self.consumer = self.rabbit_consumer

        self.ready = True
        self.logger.info("Service is ready", extra={"cloudrm_topics": self.topics, "cloudrm_backend": self.event_backend})

    def _event_backend(self) -> str:
        configured = str(get_nested(self.config, "system", "event_backend", default="")).strip().lower()
        backend = os.getenv("EVENT_BACKEND", configured or "rabbitmq").strip().lower()
        if backend in {"", "${event_backend}", "$event_backend"}:
            return "rabbitmq"
        if backend not in {"rabbitmq", "kafka", "dual"}:
            raise ValueError(f"Unsupported EVENT_BACKEND: {backend}")
        return backend

    async def stop(self) -> None:
        self.ready = False
        if self.consumer is not None:
            await self.consumer.stop()
        if self.bus is not None:
            await self.bus.stop()
        if self.rabbit is not None:
            await self.rabbit.close()
        if self.redis is not None:
            await self.redis.aclose()
        if self.db is not None:
            await self.db.dispose()
        self.logger.info("Service stopped")

    async def publish(self, topic_key: str, envelope: EventEnvelope) -> None:
        if self.db is None:
            raise RuntimeError("Runtime is not ready")
        topic = get_nested(self.config, "kafka", "topics", topic_key)
        await record_event(self.db, envelope)
        if self.event_backend in {"kafka", "dual"}:
            if self.bus is None:
                raise RuntimeError("Kafka producer is not ready")
            await self.bus.publish(topic, envelope)
            KAFKA_EVENTS.labels(topic, self.service_name).inc()
        if self.event_backend in {"rabbitmq", "dual"}:
            if self.rabbit is None:
                raise RuntimeError("RabbitMQ publisher is not ready")
            await self.rabbit.publish(envelope.event_type, envelope)
            RABBITMQ_EVENTS.labels(envelope.event_type, self.service_name).inc()

    async def health(self) -> HealthResponse:
        return HealthResponse(
            service=os.getenv("SERVICE_NAME", self.service_name),
            status="ok",
            components=[HealthComponent(name="process", status="ok")],
        )

    async def ready_response(self) -> HealthResponse:
        components: list[HealthComponent] = []
        if self.db is None or self.redis is None:
            return HealthResponse(
                service=self.service_name,
                status="down",
                components=[HealthComponent(name="runtime", status="down", details={"error": "not started"})],
            )
        try:
            await ping_db(self.db)
            components.append(HealthComponent(name="postgres", status="ok"))
        except Exception as exc:  # noqa: BLE001
            components.append(HealthComponent(name="postgres", status="down", details={"error": str(exc)}))
        try:
            await ping_redis(self.redis)
            components.append(HealthComponent(name="redis", status="ok"))
        except Exception as exc:  # noqa: BLE001
            components.append(HealthComponent(name="redis", status="down", details={"error": str(exc)}))
        if self.event_backend in {"kafka", "dual"}:
            try:
                await ping_kafka(get_nested(self.config, "kafka", "bootstrap_servers"))
                components.append(HealthComponent(name="kafka", status="ok"))
            except Exception as exc:  # noqa: BLE001
                components.append(HealthComponent(name="kafka", status="down", details={"error": str(exc)}))
        if self.event_backend in {"rabbitmq", "dual"}:
            try:
                if self.rabbit_settings is None:
                    raise RuntimeError("RabbitMQ settings are not loaded")
                await ping_rabbitmq(self.rabbit_settings)
                components.append(HealthComponent(name="rabbitmq", status="ok"))
            except Exception as exc:  # noqa: BLE001
                components.append(HealthComponent(name="rabbitmq", status="down", details={"error": str(exc)}))
        if self.last_error:
            components.append(HealthComponent(name="worker", status="degraded", details={"error": self.last_error}))
        else:
            components.append(HealthComponent(name="worker", status="ok"))
        status = "ok" if self.ready and all(component.status == "ok" for component in components) else "degraded"
        return HealthResponse(service=os.getenv("SERVICE_NAME", self.service_name), status=status, components=components)

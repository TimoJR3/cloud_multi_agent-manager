from __future__ import annotations

import logging
import os
from typing import Any

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncEngine

from cloudrm.cache import create_redis, ping_redis
from cloudrm.config import get_nested, load_config
from cloudrm.db import create_engine, ping_db, run_migrations
from cloudrm.kafka import EventBus, EventConsumer, ping_kafka
from cloudrm.messages import EventEnvelope, HealthComponent, HealthResponse
from cloudrm.metrics import KAFKA_EVENTS, RABBITMQ_EVENTS
from cloudrm.persistence import record_event
from cloudrm.rabbit import RabbitMQSettings, RabbitPublisher, ping_rabbitmq
from cloudrm.retry import retry_async


class ServiceRuntime:
    def __init__(self, service_name: str, topics: list[str] | None = None) -> None:
        self.service_name = service_name
        self.topics = topics or []
        self.config: dict[str, Any] = {}
        self.db: AsyncEngine | None = None
        self.redis: Redis | None = None
        self.bus: EventBus | None = None
        self.consumer: EventConsumer | None = None
        self.rabbit_settings: RabbitMQSettings | None = None
        self.rabbit: RabbitPublisher | None = None
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

        self.logger.info("Запуск сервиса: проверка зависимостей")
        self.db = create_engine(postgres_dsn)
        await retry_async(lambda: ping_db(self.db), attempts=attempts, delay_seconds=delay, name="postgres")
        await run_migrations(self.db)

        self.redis = create_redis(redis_url)
        await retry_async(lambda: ping_redis(self.redis), attempts=attempts, delay_seconds=delay, name="redis")

        await retry_async(lambda: ping_kafka(kafka_servers), attempts=attempts, delay_seconds=delay, name="kafka")
        self.bus = EventBus(kafka_servers, self.service_name)
        await self.bus.start()
        await retry_async(lambda: ping_rabbitmq(self.rabbit_settings), attempts=attempts, delay_seconds=delay, name="rabbitmq")
        self.rabbit = RabbitPublisher(self.rabbit_settings, self.service_name)
        await self.rabbit.start(attempts=attempts, delay_seconds=delay)
        if self.topics:
            self.consumer = EventConsumer(kafka_servers, self.service_name, self.topics)
            await self.consumer.start()
        self.ready = True
        self.logger.info("Сервис готов", extra={"cloudrm_topics": self.topics})

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
        self.logger.info("Сервис остановлен")

    async def publish(self, topic_key: str, envelope: EventEnvelope) -> None:
        if self.bus is None or self.rabbit is None or self.db is None:
            raise RuntimeError("Runtime не готов")
        topic = get_nested(self.config, "kafka", "topics", topic_key)
        await record_event(self.db, envelope)
        await self.bus.publish(topic, envelope)
        await self.rabbit.publish(envelope.event_type, envelope)
        KAFKA_EVENTS.labels(topic, self.service_name).inc()
        RABBITMQ_EVENTS.labels(envelope.event_type, self.service_name).inc()

    async def health(self) -> HealthResponse:
        return HealthResponse(
            service=os.getenv("SERVICE_NAME", self.service_name),
            status="ok",
            components=[HealthComponent(name="process", status="ok")],
        )

    async def ready(self) -> HealthResponse:
        components: list[HealthComponent] = []
        if self.db is None or self.redis is None:
            return HealthResponse(
                service=self.service_name,
                status="down",
                components=[HealthComponent(name="runtime", status="down", details={"error": "не запущен"})],
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
        try:
            await ping_kafka(get_nested(self.config, "kafka", "bootstrap_servers"))
            components.append(HealthComponent(name="kafka", status="ok"))
        except Exception as exc:  # noqa: BLE001
            components.append(HealthComponent(name="kafka", status="down", details={"error": str(exc)}))
        try:
            if self.rabbit_settings is None:
                raise RuntimeError("настройки RabbitMQ не загружены")
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

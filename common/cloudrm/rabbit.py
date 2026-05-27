from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from aio_pika import DeliveryMode, ExchangeType, IncomingMessage, Message, connect_robust
from aio_pika.abc import AbstractChannel, AbstractExchange, AbstractQueue, AbstractRobustConnection
from pydantic import BaseModel, Field

from cloudrm.config import get_nested
from cloudrm.messages import EventEnvelope

logger = logging.getLogger("cloudrm.rabbitmq")


class RabbitMQSettings(BaseModel):
    host: str = "rabbitmq"
    port: int = 5672
    username: str = "mas"
    password: str = "mas_password"
    exchange: str = "mas.events"
    dlx: str = "mas.dlx"
    retry_exchange: str = "mas.retry"
    prefetch: int = 10
    retry_ttl_ms: int = 10000
    default_ttl_seconds: int = 300
    queues: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "queue.requests": ["request.*", "execution.*"],
            "queue.resources": ["need_placement", "node.metrics.*", "node.failure.*"],
            "queue.sla": ["request.classified", "decision.*", "execution.*"],
            "queue.forecast": ["request.*", "queue.*", "execution.*"],
            "queue.coordinator": ["request.classified", "node.proposal.*", "sla.*", "forecast.*"],
            "queue.executor": ["decision.*"],
        }
    )

    @property
    def amqp_url(self) -> str:
        return f"amqp://{self.username}:{self.password}@{self.host}:{self.port}/"

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "RabbitMQSettings":
        raw = get_nested(config, "rabbitmq", default={})
        return cls(
            host=str(raw.get("host", "rabbitmq")),
            port=int(raw.get("port", 5672)),
            username=str(raw.get("username", "mas")),
            password=str(raw.get("password", "mas_password")),
            exchange=str(raw.get("exchange", "mas.events")),
            dlx=str(raw.get("dlx", "mas.dlx")),
            retry_exchange=str(raw.get("retry_exchange", "mas.retry")),
            prefetch=int(raw.get("prefetch", 10)),
            retry_ttl_ms=int(raw.get("retry_ttl_ms", 10000)),
            default_ttl_seconds=int(raw.get("default_ttl_seconds", 300)),
        )


async def connect_with_retry(settings: RabbitMQSettings, attempts: int, delay_seconds: float) -> AbstractRobustConnection:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            connection = await connect_robust(settings.amqp_url)
            logger.info("RabbitMQ подключен", extra={"cloudrm_attempt": attempt, "cloudrm_host": settings.host})
            return connection
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            logger.warning(
                "RabbitMQ пока недоступен",
                extra={"cloudrm_attempt": attempt, "cloudrm_error": str(exc), "cloudrm_host": settings.host},
            )
            await asyncio.sleep(delay_seconds)
    raise RuntimeError("RabbitMQ не стал доступен после повторных попыток") from last_error


async def declare_topology(channel: AbstractChannel, settings: RabbitMQSettings) -> dict[str, AbstractExchange | AbstractQueue]:
    events = await channel.declare_exchange(settings.exchange, ExchangeType.TOPIC, durable=True)
    dlx = await channel.declare_exchange(settings.dlx, ExchangeType.TOPIC, durable=True)
    retry = await channel.declare_exchange(settings.retry_exchange, ExchangeType.TOPIC, durable=True)

    declared: dict[str, AbstractExchange | AbstractQueue] = {
        settings.exchange: events,
        settings.dlx: dlx,
        settings.retry_exchange: retry,
    }
    queue_arguments = {"x-dead-letter-exchange": settings.dlx}
    for queue_name, bindings in settings.queues.items():
        queue = await channel.declare_queue(queue_name, durable=True, arguments=queue_arguments)
        declared[queue_name] = queue
        for routing_key in bindings:
            await queue.bind(events, routing_key=routing_key)

    dead_queue = await channel.declare_queue("queue.dead", durable=True)
    await dead_queue.bind(dlx, routing_key="#")
    declared["queue.dead"] = dead_queue

    retry_queue = await channel.declare_queue(
        "queue.retry",
        durable=True,
        arguments={"x-message-ttl": settings.retry_ttl_ms, "x-dead-letter-exchange": settings.exchange},
    )
    await retry_queue.bind(retry, routing_key="#")
    declared["queue.retry"] = retry_queue

    logger.info("RabbitMQ топология объявлена", extra={"cloudrm_exchange": settings.exchange})
    return declared


class RabbitPublisher:
    def __init__(self, settings: RabbitMQSettings, service_name: str) -> None:
        self.settings = settings
        self.service_name = service_name
        self.connection: AbstractRobustConnection | None = None
        self.channel: AbstractChannel | None = None
        self.exchange: AbstractExchange | None = None

    async def start(self, attempts: int = 30, delay_seconds: float = 2.0) -> None:
        self.connection = await connect_with_retry(self.settings, attempts, delay_seconds)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.settings.prefetch)
        topology = await declare_topology(self.channel, self.settings)
        self.exchange = topology[self.settings.exchange]  # type: ignore[assignment]

    async def publish(self, routing_key: str, envelope: EventEnvelope, ttl_seconds: int | None = None) -> str:
        if self.exchange is None:
            raise RuntimeError("RabbitMQ publisher не запущен")
        broker_payload = envelope.broker_payload()
        message_id = str(broker_payload["message_id"])
        ttl = ttl_seconds or envelope.ttl_seconds or self.settings.default_ttl_seconds
        message = Message(
            json.dumps(broker_payload, ensure_ascii=False, default=str).encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=message_id,
            correlation_id=envelope.correlation_id,
            expiration=ttl * 1000,
            headers={
                "event_id": envelope.event_id,
                "schema_version": envelope.schema_version,
                "source": envelope.source,
            },
        )
        await self.exchange.publish(message, routing_key=routing_key)
        logger.info(
            "Сообщение опубликовано в RabbitMQ",
            extra={"cloudrm_routing_key": routing_key, "cloudrm_correlation_id": envelope.correlation_id},
        )
        return message_id

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()


class RabbitConsumer:
    def __init__(self, settings: RabbitMQSettings, queue_name: str, service_name: str) -> None:
        self.settings = settings
        self.queue_name = queue_name
        self.service_name = service_name
        self.connection: AbstractRobustConnection | None = None
        self.channel: AbstractChannel | None = None
        self.queue: AbstractQueue | None = None

    async def start(self, attempts: int = 30, delay_seconds: float = 2.0) -> None:
        self.connection = await connect_with_retry(self.settings, attempts, delay_seconds)
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=self.settings.prefetch)
        topology = await declare_topology(self.channel, self.settings)
        queue = topology.get(self.queue_name)
        if queue is None:
            raise RuntimeError(f"RabbitMQ очередь {self.queue_name} не объявлена")
        self.queue = queue  # type: ignore[assignment]

    async def consume(self, handler: Callable[[EventEnvelope, IncomingMessage], Awaitable[None]]) -> None:
        if self.queue is None:
            raise RuntimeError("RabbitMQ consumer не запущен")

        async def wrapped(message: IncomingMessage) -> None:
            async with message.process(requeue=False):
                payload = json.loads(message.body.decode("utf-8"))
                envelope = EventEnvelope.model_validate(payload)
                if envelope.is_expired():
                    logger.warning(
                        "Просроченное RabbitMQ сообщение пропущено",
                        extra={"cloudrm_correlation_id": envelope.correlation_id, "cloudrm_event_type": envelope.event_type},
                    )
                    return
                await handler(envelope, message)

        await self.queue.consume(wrapped)

    async def close(self) -> None:
        if self.connection is not None:
            await self.connection.close()


async def ping_rabbitmq(settings: RabbitMQSettings) -> bool:
    connection = await connect_robust(settings.amqp_url)
    await connection.close()
    return True

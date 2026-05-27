from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from aio_pika import DeliveryMode, Message, connect_robust
from aiokafka import AIOKafkaProducer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "common"))

from cloudrm.messages import EventEnvelope  # noqa: E402
from cloudrm.rabbit import RabbitMQSettings, declare_topology  # noqa: E402


def load_env() -> dict[str, str]:
    env_path = ROOT / ".env"
    values: dict[str, str] = {}
    if not env_path.exists():
        print("[!] .env не найден. Выполните: make init")
        return values
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key] = value.strip().strip("\"'")
        os.environ.setdefault(key, values[key])
    return values


def local_kafka_bootstrap(value: str) -> str:
    return value.replace("kafka:9092", "localhost:9092")


def local_rabbit_settings() -> RabbitMQSettings:
    host = os.getenv("RABBITMQ_HOST", "localhost")
    if host == "rabbitmq":
        host = "localhost"
    return RabbitMQSettings(
        host=host,
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        username=os.getenv("RABBITMQ_DEFAULT_USER", "mas"),
        password=os.getenv("RABBITMQ_DEFAULT_PASS", "mas_password"),
        exchange=os.getenv("RABBITMQ_EXCHANGE", "mas.events"),
        dlx=os.getenv("RABBITMQ_DLX", "mas.dlx"),
        retry_exchange=os.getenv("RABBITMQ_RETRY_EXCHANGE", "mas.retry"),
        prefetch=int(os.getenv("RABBITMQ_PREFETCH", "10")),
    )


async def validate_kafka() -> bool:
    bootstrap = local_kafka_bootstrap(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    envelope = EventEnvelope(event_type="diagnostics.kafka", source="validate-brokers", payload={"status": "test"})
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    try:
        await producer.start()
        value = json.dumps(envelope.broker_payload(), ensure_ascii=False, default=str).encode("utf-8")
        await producer.send_and_wait("telemetry.events", value, key=envelope.correlation_id.encode("utf-8"))
        print("[✓] Kafka доступен, тестовое сообщение опубликовано в telemetry.events")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"[!] Kafka недоступен или не принял сообщение: {exc}")
        return False
    finally:
        try:
            await producer.stop()
        except Exception:
            pass


async def validate_rabbitmq() -> bool:
    settings = local_rabbit_settings()
    envelope = EventEnvelope(event_type="diagnostics.rabbitmq", source="validate-brokers", payload={"status": "test"})
    try:
        connection = await connect_robust(settings.amqp_url)
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=settings.prefetch)
        topology = await declare_topology(channel, settings)
        exchange = topology[settings.exchange]
        diagnostic_queue = await channel.declare_queue("", exclusive=True, auto_delete=True)
        await diagnostic_queue.bind(exchange, routing_key="diagnostics.rabbitmq")
        payload = envelope.broker_payload()
        message = Message(
            json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8"),
            content_type="application/json",
            delivery_mode=DeliveryMode.PERSISTENT,
            message_id=payload["message_id"],
            correlation_id=envelope.correlation_id,
            expiration=settings.default_ttl_seconds * 1000,
            headers={"event_id": envelope.event_id, "schema_version": envelope.schema_version},
        )
        await exchange.publish(message, routing_key="diagnostics.rabbitmq")
        incoming = await diagnostic_queue.get(timeout=5, fail=False)
        if incoming is None:
            print("[!] RabbitMQ доступен, но тестовое сообщение не прочитано из diagnostic queue")
            await connection.close()
            return False
        async with incoming.process():
            received = json.loads(incoming.body.decode("utf-8"))
        await connection.close()
        if received.get("correlation_id") != envelope.correlation_id:
            print("[!] RabbitMQ вернул сообщение с неожиданным correlation_id")
            return False
        print("[✓] RabbitMQ доступен, топология объявлена, тестовое сообщение опубликовано и прочитано")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"[!] RabbitMQ недоступен или топология не проверена: {exc}")
        return False


async def main_async() -> int:
    load_env()
    results = [await validate_kafka(), await validate_rabbitmq()]
    if all(results):
        print("[✓] Итог: оба брокера прошли проверку")
        return 0
    print("[!] Итог: проверка брокеров не пройдена полностью")
    print("Команды для диагностики:")
    print("  docker compose ps")
    print("  docker compose logs --tail=200 kafka rabbitmq")
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main_async()))

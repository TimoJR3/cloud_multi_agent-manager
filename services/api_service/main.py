from __future__ import annotations

import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from cloudrm.cache import create_redis, ping_redis
from cloudrm.config import get_nested, load_config
from cloudrm.db import create_engine, ping_db, run_migrations
from cloudrm.kafka import EventBus, ping_kafka
from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthComponent, HealthResponse, TaskCreate, TaskView
from cloudrm.metrics import API_REQUESTS, KAFKA_EVENTS, RABBITMQ_EVENTS, TASKS_SUBMITTED
from cloudrm.rabbit import RabbitMQSettings, RabbitPublisher, ping_rabbitmq
from cloudrm.retry import retry_async

configure_logging()
logger = logging.getLogger("api-service")


class AppState:
    config: dict[str, Any]
    db: AsyncEngine
    redis: Redis
    bus: EventBus | None
    rabbit_settings: RabbitMQSettings
    rabbit: RabbitPublisher | None
    event_backend: str


state = AppState()


@asynccontextmanager
async def lifespan(_: FastAPI):
    service_name = os.getenv("SERVICE_NAME", "api-service")
    state.config = load_config()
    retry = get_nested(state.config, "system", "retry", default={})
    attempts = int(retry.get("attempts", 30))
    delay = float(retry.get("delay_seconds", 2))

    postgres_dsn = get_nested(state.config, "postgres", "dsn")
    redis_url = get_nested(state.config, "redis", "url")
    kafka_servers = get_nested(state.config, "kafka", "bootstrap_servers")
    state.rabbit_settings = RabbitMQSettings.from_config(state.config)
    configured_backend = str(get_nested(state.config, "system", "event_backend", default="")).strip().lower()
    state.event_backend = os.getenv("EVENT_BACKEND", configured_backend or "rabbitmq").strip().lower()
    if state.event_backend in {"", "${event_backend}", "$event_backend"}:
        state.event_backend = "rabbitmq"

    logger.info("Запуск API-сервиса: проверка зависимостей")
    state.db = create_engine(postgres_dsn)
    await retry_async(lambda: ping_db(state.db), attempts=attempts, delay_seconds=delay, name="postgres")
    await run_migrations(state.db)

    state.redis = create_redis(redis_url)
    await retry_async(lambda: ping_redis(state.redis), attempts=attempts, delay_seconds=delay, name="redis")

    state.bus = None
    state.rabbit = None
    if state.event_backend in {"kafka", "dual"}:
        await retry_async(lambda: ping_kafka(kafka_servers), attempts=attempts, delay_seconds=delay, name="kafka")
        state.bus = EventBus(kafka_servers, service_name)
        await state.bus.start()
    if state.event_backend in {"rabbitmq", "dual"}:
        await retry_async(lambda: ping_rabbitmq(state.rabbit_settings), attempts=attempts, delay_seconds=delay, name="rabbitmq")
        state.rabbit = RabbitPublisher(state.rabbit_settings, service_name)
        await state.rabbit.start(attempts=attempts, delay_seconds=delay)
    logger.info("API-сервис готов к работе")
    try:
        yield
    finally:
        if state.bus is not None:
            await state.bus.stop()
        if state.rabbit is not None:
            await state.rabbit.close()
        await state.redis.aclose()
        await state.db.dispose()
        logger.info("API-сервис остановлен")


app = FastAPI(title="CloudRM API", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    response = await call_next(request)
    API_REQUESTS.labels(request.method, request.url.path).inc()
    return response


async def record_event(envelope: EventEnvelope) -> None:
    async with state.db.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO events(event_id, correlation_id, event_type, source, payload, occurred_at)
                VALUES (:event_id, :correlation_id, :event_type, :source, CAST(:payload AS JSONB), :occurred_at)
                """
            ),
            {
                "event_id": envelope.event_id,
                "correlation_id": envelope.correlation_id,
                "event_type": envelope.event_type,
                "source": envelope.source,
                "payload": json.dumps(envelope.payload, ensure_ascii=False),
                "occurred_at": envelope.occurred_at,
            },
        )


async def publish_event(topic_key: str, envelope: EventEnvelope) -> None:
    await record_event(envelope)
    if state.event_backend in {"kafka", "dual"}:
        if state.bus is None:
            raise RuntimeError("Kafka producer is not ready")
        topic = get_nested(state.config, "kafka", "topics", topic_key)
        await state.bus.publish(topic, envelope)
        KAFKA_EVENTS.labels(topic, "api-service").inc()
    if state.event_backend in {"rabbitmq", "dual"}:
        if state.rabbit is None:
            raise RuntimeError("RabbitMQ publisher is not ready")
        await state.rabbit.publish(envelope.event_type, envelope)
        RABBITMQ_EVENTS.labels(envelope.event_type, "api-service").inc()


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(service=os.getenv("SERVICE_NAME", "api-service"), status="ok", components=[HealthComponent(name="process", status="ok")])


@app.get("/ready", response_model=HealthResponse)
async def ready(response: Response) -> HealthResponse:
    components: list[HealthComponent] = []
    try:
        await ping_db(state.db)
        components.append(HealthComponent(name="postgres", status="ok"))
    except Exception as exc:  # noqa: BLE001
        components.append(HealthComponent(name="postgres", status="down", details={"error": str(exc)}))
    try:
        await ping_redis(state.redis)
        components.append(HealthComponent(name="redis", status="ok"))
    except Exception as exc:  # noqa: BLE001
        components.append(HealthComponent(name="redis", status="down", details={"error": str(exc)}))
    if state.event_backend in {"kafka", "dual"}:
        try:
            await ping_kafka(get_nested(state.config, "kafka", "bootstrap_servers"))
            components.append(HealthComponent(name="kafka", status="ok"))
        except Exception as exc:  # noqa: BLE001
            components.append(HealthComponent(name="kafka", status="down", details={"error": str(exc)}))
    if state.event_backend in {"rabbitmq", "dual"}:
        try:
            await ping_rabbitmq(state.rabbit_settings)
            components.append(HealthComponent(name="rabbitmq", status="ok"))
        except Exception as exc:  # noqa: BLE001
            components.append(HealthComponent(name="rabbitmq", status="down", details={"error": str(exc)}))

    status = "ok" if all(component.status == "ok" for component in components) else "degraded"
    if status != "ok":
        response.status_code = 503
    return HealthResponse(service=os.getenv("SERVICE_NAME", "api-service"), status=status, components=components)


@app.post("/tasks", status_code=202)
async def submit_task(task: TaskCreate) -> dict[str, str]:
    task_id = str(uuid4())
    correlation_id = str(uuid4())
    now = datetime.now(UTC)
    async with state.db.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO tasks(
                    task_id, correlation_id, status, task_type, priority, cpu_required, ram_required_mb,
                    duration_seconds, sla_deadline_seconds, requires_gpu, payload, created_at, updated_at
                )
                VALUES (
                    :task_id, :correlation_id, 'created', :task_type, :priority, :cpu_required, :ram_required_mb,
                    :duration_seconds, :sla_deadline_seconds, :requires_gpu, CAST(:payload AS JSONB), :created_at, :updated_at
                )
                """
            ),
            {
                "task_id": task_id,
                "correlation_id": correlation_id,
                "task_type": task.task_type,
                "priority": task.priority,
                "cpu_required": task.cpu_required,
                "ram_required_mb": task.ram_required_mb,
                "duration_seconds": task.duration_seconds,
                "sla_deadline_seconds": task.sla_deadline_seconds,
                "requires_gpu": task.requires_gpu,
                "payload": json.dumps(task.payload, ensure_ascii=False),
                "created_at": now,
                "updated_at": now,
            },
        )
    await state.redis.hset(
        f"task:{task_id}",
        mapping={"status": "created", "priority": str(task.priority), "created_at": now.isoformat(), "created_at_ts": str(now.timestamp()), "correlation_id": correlation_id},
    )
    envelope = EventEnvelope(
        event_type="request.created",
        correlation_id=correlation_id,
        source="api-service",
        payload={"task_id": task_id, "created_at": now.isoformat(), "created_at_ts": now.timestamp(), **task.model_dump(mode="json")},
    )
    await publish_event("request_created", envelope)
    TASKS_SUBMITTED.inc()
    return {"task_id": task_id, "correlation_id": correlation_id, "status": "accepted"}


@app.get("/tasks/{task_id}", response_model=TaskView)
async def get_task(task_id: str) -> TaskView:
    async with state.db.connect() as connection:
        result = await connection.execute(text("SELECT * FROM tasks WHERE task_id = :task_id"), {"task_id": task_id})
        row = result.mappings().first()
    if row is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    data = dict(row)
    data["task_id"] = str(data["task_id"])
    return TaskView(**data)


@app.get("/nodes")
async def list_nodes() -> list[dict[str, Any]]:
    keys = await state.redis.keys("node:*")
    nodes: list[dict[str, Any]] = []
    for key in keys:
        node = await state.redis.hgetall(key)
        if node:
            nodes.append(node)
    if nodes:
        return nodes
    configured_nodes = get_nested(state.config, "agents", "resource", "nodes", default=[])
    return configured_nodes


@app.post("/experiments/start")
async def start_experiment(payload: dict[str, Any] | None = None) -> dict[str, str]:
    scenario = (payload or {}).get("scenario", "normal")
    mode = (payload or {}).get("mode", "mas")
    await state.redis.set("experiment:active", scenario)
    await state.redis.set("experiment:mode", mode)
    return {"status": "started", "scenario": scenario, "mode": mode}


@app.post("/experiments/stop")
async def stop_experiment() -> dict[str, str]:
    await state.redis.delete("experiment:active")
    await state.redis.delete("experiment:mode")
    return {"status": "stopped"}


@app.post("/nodes/{node_id}/failure")
async def simulate_node_failure(node_id: str) -> dict[str, str]:
    await state.redis.hset(f"node:{node_id}", mapping={"node_id": node_id, "status": "failed"})
    async with state.db.begin() as connection:
        await connection.execute(
            text(
                """
                UPDATE tasks
                SET status = 'failed', error = 'node_failure', updated_at = :updated_at
                WHERE assigned_node_id = :node_id AND status = 'running'
                """
            ),
            {"node_id": node_id, "updated_at": datetime.now(UTC)},
        )
    for task_key in await state.redis.keys("task:*"):
        task = await state.redis.hgetall(task_key)
        if task.get("node_id") == node_id and task.get("status") == "running":
            await state.redis.hset(task_key, mapping={"status": "failed", "error": "node_failure"})
        if task.get("class") == "critical" and task.get("status") in {"failed", "need_restart"}:
            task_id = task_key.split(":", 1)[1]
            await state.redis.hset(task_key, mapping={"status": "classified"})
            await state.redis.zadd("queue:waiting", {task_id: float(task.get("priority", 10)) + 5})
            await state.redis.zadd("queue:waiting:critical", {task_id: float(task.get("priority", 10)) + 5})
    envelope = EventEnvelope(
        event_type="node.unavailable",
        correlation_id=str(uuid4()),
        source="api-service",
        payload={"node_id": node_id, "reason": "manual_failure", "status": "failed"},
    )
    await publish_event("node_unavailable", envelope)
    return {"status": "failure-simulated", "node_id": node_id}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

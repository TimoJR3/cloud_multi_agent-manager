from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, EXECUTION_FAILURES, NODE_UTILIZATION, QUEUE_LENGTH, REQUEST_WAIT_SECONDS
from cloudrm.persistence import save_execution, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("executor-agent")

runtime = ServiceRuntime("executor-agent", ["decision.dispatch", "node.unavailable"])
worker_task: asyncio.Task[None] | None = None

TASK_CLASSES = ("standard", "cpu-heavy", "memory-heavy", "gpu", "critical")


async def reserve(node_id: str, cpu: float, ram: int, sign: int) -> None:
    assert runtime.redis is not None
    key = f"node:{node_id}"
    node = await runtime.redis.hgetall(key)
    if sign > 0 and node.get("status") == "failed":
        raise RuntimeError("node_failure")
    cpu_used = max(0.0, float(node.get("cpu_used", 0)) + sign * cpu)
    ram_used = max(0.0, float(node.get("ram_used_mb", 0)) + sign * ram)
    await runtime.redis.hset(key, mapping={"cpu_used": str(cpu_used), "ram_used_mb": str(ram_used)})
    if node.get("cpu_total"):
        NODE_UTILIZATION.labels(node_id, "cpu").set(cpu_used / float(node["cpu_total"]))
    if node.get("ram_total_mb"):
        NODE_UTILIZATION.labels(node_id, "ram").set(ram_used / float(node["ram_total_mb"]))


async def remove_from_waiting(task_id: str, task_class: str) -> None:
    assert runtime.redis is not None
    await runtime.redis.zrem("queue:waiting", task_id)
    await runtime.redis.zrem(f"queue:waiting:{task_class}", task_id)
    QUEUE_LENGTH.labels("waiting").set(await runtime.redis.zcard("queue:waiting"))
    QUEUE_LENGTH.labels(task_class).set(await runtime.redis.zcard(f"queue:waiting:{task_class}"))


async def fail_tasks_on_node(node_id: str, reason: str, correlation_id: str) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    task_ids = await runtime.redis.smembers(f"node:{node_id}:running")
    for task_id in task_ids:
        await update_task(runtime.db, task_id, status="failed", error=reason)
        await runtime.redis.hset(f"task:{task_id}", mapping={"status": "failed", "error": reason})
        await runtime.publish(
            "execution_failed",
            EventEnvelope(
                event_type="execution.failed",
                correlation_id=correlation_id,
                source="executor-agent",
                payload={"task_id": task_id, "node_id": node_id, "status": "failed", "reason": reason},
            ),
        )
    await runtime.redis.delete(f"node:{node_id}:running")


async def execute_task(envelope: EventEnvelope) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    task = envelope.payload
    task_id = task["task_id"]
    node_id = task["node_id"]
    task_class = task.get("class", "standard")
    started_at = datetime.now(UTC)
    reserved = False
    cpu = float(task.get("cpu_required", 1))
    ram = int(task.get("ram_required_mb", 512))
    try:
        await remove_from_waiting(task_id, task_class)
        created_ts = float(task.get("created_at_ts", time.time()))
        REQUEST_WAIT_SECONDS.labels(task_class).observe(max(0.0, time.time() - created_ts))
        await reserve(node_id, cpu, ram, +1)
        reserved = True
        await runtime.redis.sadd(f"node:{node_id}:running", task_id)
        await update_task(runtime.db, task_id, status="running", assigned_node_id=node_id)
        await runtime.redis.hset(
            f"task:{task_id}",
            mapping={"status": "running", "node_id": node_id, "started_at": started_at.isoformat(), "correlation_id": envelope.correlation_id},
        )
        remaining = min(float(task.get("duration_seconds", 1)), 10.0)
        while remaining > 0:
            await asyncio.sleep(min(0.5, remaining))
            remaining -= 0.5
            node = await runtime.redis.hgetall(f"node:{node_id}")
            if node.get("status") == "failed":
                raise RuntimeError("node_failure")
        finished_at = datetime.now(UTC)
        await reserve(node_id, cpu, ram, -1)
        reserved = False
        await runtime.redis.srem(f"node:{node_id}:running", task_id)
        await update_task(runtime.db, task_id, status="done")
        await runtime.redis.hset(f"task:{task_id}", mapping={"status": "done", "finished_at": finished_at.isoformat()})
        await save_execution(
            runtime.db,
            task_id=task_id,
            correlation_id=envelope.correlation_id,
            node_id=node_id,
            status="done",
            started_at=started_at,
            finished_at=finished_at,
            details={"adapter": "kubernetes-emulator"},
        )
        await runtime.publish(
            "execution_done",
            EventEnvelope(
                event_type="execution.done",
                correlation_id=envelope.correlation_id,
                source="executor-agent",
                payload={**task, "status": "done", "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat()},
            ),
        )
        logger.info("Task executed", extra={"cloudrm_task_id": task_id, "cloudrm_node_id": node_id})
    except Exception as exc:  # noqa: BLE001
        finished_at = datetime.now(UTC)
        reason = "node_failure" if str(exc) == "node_failure" else str(exc)
        runtime.last_error = reason
        EXECUTION_FAILURES.labels(reason).inc()
        if reserved:
            await reserve(node_id, cpu, ram, -1)
            await runtime.redis.srem(f"node:{node_id}:running", task_id)
        await update_task(runtime.db, task_id, status="failed", error=reason)
        await runtime.redis.hset(f"task:{task_id}", mapping={"status": "failed", "error": reason})
        await save_execution(
            runtime.db,
            task_id=task_id,
            correlation_id=envelope.correlation_id,
            node_id=node_id,
            status="failed",
            started_at=started_at,
            finished_at=finished_at,
            details={"reason": reason},
        )
        await runtime.publish(
            "execution_failed",
            EventEnvelope(
                event_type="execution.failed",
                correlation_id=envelope.correlation_id,
                source="executor-agent",
                payload={**task, "status": "failed", "reason": reason, "started_at": started_at.isoformat(), "finished_at": finished_at.isoformat()},
            ),
        )
        logger.exception("Task execution failed")


async def worker() -> None:
    assert runtime.consumer is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            if envelope.event_type == "decision.dispatch":
                await execute_task(envelope)
            elif envelope.event_type == "node.unavailable":
                await fail_tasks_on_node(envelope.payload["node_id"], "node_failure", envelope.correlation_id)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Executor event processing failed")
        finally:
            AGENT_LATENCY.labels("executor-agent").observe(time.perf_counter() - started)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task
    await runtime.start()
    worker_task = asyncio.create_task(worker())
    try:
        yield
    finally:
        if worker_task is not None:
            worker_task.cancel()
        await runtime.stop()


app = FastAPI(title="CloudRM Executor Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()


@app.get("/ready")
async def ready():
    if runtime.ready:
        return {"status": "ok", "service": "executor-agent", "ready": True}
    return JSONResponse(status_code=503, content={"status": "not_ready", "service": "executor-agent", "ready": False})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

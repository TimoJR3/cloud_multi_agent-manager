from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, NODE_UTILIZATION, QUEUE_LENGTH
from cloudrm.persistence import save_execution, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("executor-agent")

runtime = ServiceRuntime("executor-agent", ["decision.dispatch"])
worker_task: asyncio.Task[None] | None = None


async def reserve(node_id: str, cpu: float, ram: int, sign: int) -> None:
    assert runtime.redis is not None
    key = f"node:{node_id}"
    node = await runtime.redis.hgetall(key)
    cpu_used = max(0.0, float(node.get("cpu_used", 0)) + sign * cpu)
    ram_used = max(0.0, float(node.get("ram_used_mb", 0)) + sign * ram)
    await runtime.redis.hset(key, mapping={"cpu_used": str(cpu_used), "ram_used_mb": str(ram_used)})
    if node.get("cpu_total"):
        NODE_UTILIZATION.labels(node_id, "cpu").set(cpu_used / float(node["cpu_total"]))
    if node.get("ram_total_mb"):
        NODE_UTILIZATION.labels(node_id, "ram").set(ram_used / float(node["ram_total_mb"]))


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    assert runtime.db is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        task = envelope.payload
        task_id = task["task_id"]
        node_id = task["node_id"]
        started_at = datetime.now(UTC)
        try:
            cpu = float(task.get("cpu_required", 1))
            ram = int(task.get("ram_required_mb", 512))
            await runtime.redis.zrem("queue:waiting", task_id)
            QUEUE_LENGTH.labels("waiting").set(await runtime.redis.zcard("queue:waiting"))
            await reserve(node_id, cpu, ram, +1)
            await update_task(runtime.db, task_id, status="running", assigned_node_id=node_id)
            await runtime.redis.hset(f"task:{task_id}", mapping={"status": "running", "node_id": node_id})
            await asyncio.sleep(min(float(task.get("duration_seconds", 1)), 10.0))
            finished_at = datetime.now(UTC)
            await reserve(node_id, cpu, ram, -1)
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
            await runtime.consumer.commit()
            runtime.last_error = None
            logger.info("Задача исполнена", extra={"cloudrm_task_id": task_id, "cloudrm_node_id": node_id})
        except Exception as exc:  # noqa: BLE001
            finished_at = datetime.now(UTC)
            runtime.last_error = str(exc)
            await update_task(runtime.db, task_id, status="failed", error=str(exc))
            await save_execution(
                runtime.db,
                task_id=task_id,
                correlation_id=envelope.correlation_id,
                node_id=node_id,
                status="failed",
                started_at=started_at,
                finished_at=finished_at,
                details={"error": str(exc)},
            )
            await runtime.publish(
                "execution_failed",
                EventEnvelope(
                    event_type="execution.failed",
                    correlation_id=envelope.correlation_id,
                    source="executor-agent",
                    payload={**task, "status": "failed", "error": str(exc)},
                ),
            )
            await runtime.consumer.commit()
            logger.exception("Ошибка исполнения задачи")
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
        return {
            "status": "ok",
            "service": "executor-agent",
            "ready": True,
        }
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "executor-agent",
            "ready": False,
        },
    )


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

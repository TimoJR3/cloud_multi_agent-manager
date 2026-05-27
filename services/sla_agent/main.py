from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, SLA_VIOLATIONS
from cloudrm.persistence import save_sla_violation, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("sla-agent")

runtime = ServiceRuntime("sla-agent", ["request.classified"])
worker_task: asyncio.Task[None] | None = None


async def worker() -> None:
    assert runtime.consumer is not None
    assert runtime.redis is not None
    assert runtime.db is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            task = envelope.payload
            queue_length = int(task.get("queue_length", 0))
            duration = float(task.get("duration_seconds", 1))
            deadline = float(task.get("sla_deadline_seconds", 30))
            estimated_wait = queue_length * 0.5
            risk = min(1.0, max(0.0, (estimated_wait + duration) / deadline))
            severity = "high" if risk >= 0.8 else "medium" if risk >= 0.5 else "low"
            unsafe = risk >= 0.95
            boosted_priority = min(10, int(task.get("dynamic_priority", task.get("priority", 1))) + (2 if risk >= 0.8 else 0))
            if boosted_priority != int(task.get("dynamic_priority", task.get("priority", 1))):
                await update_task(runtime.db, task["task_id"], priority=boosted_priority)
            if risk >= 0.8:
                SLA_VIOLATIONS.inc()
                await save_sla_violation(
                    runtime.db,
                    task_id=task["task_id"],
                    correlation_id=envelope.correlation_id,
                    severity=severity,
                    details={"risk": risk, "estimated_wait": estimated_wait},
                )
            await runtime.redis.hset(
                f"sla:{task['task_id']}",
                mapping={"risk": str(risk), "severity": severity, "unsafe": str(unsafe).lower()},
            )
            await runtime.publish(
                "sla_risk",
                EventEnvelope(
                    event_type="sla.risk",
                    correlation_id=envelope.correlation_id,
                    source="sla-agent",
                    payload={**task, "sla_risk": round(risk, 4), "sla_severity": severity, "unsafe": unsafe, "boosted_priority": boosted_priority},
                ),
            )
            await runtime.consumer.commit()
            runtime.last_error = None
            logger.info("Оценен SLA-риск", extra={"cloudrm_task_id": task["task_id"], "cloudrm_risk": risk})
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("Ошибка SLA-агента")
        finally:
            AGENT_LATENCY.labels("sla-agent").observe(time.perf_counter() - started)


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


app = FastAPI(title="CloudRM SLA Agent", lifespan=lifespan)


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return await runtime.health()



@app.get("/ready")
async def ready():
    if runtime.ready:
        return {
            "status": "ok",
            "service": "sla-agent",
            "ready": True,
        }

    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "service": "sla-agent",
            "ready": False,
        },
    )

@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

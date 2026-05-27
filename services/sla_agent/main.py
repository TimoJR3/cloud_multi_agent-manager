from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from cloudrm.logging import configure_logging
from cloudrm.messages import EventEnvelope, HealthResponse
from cloudrm.metrics import AGENT_LATENCY, SLA_RISKS, SLA_VIOLATIONS
from cloudrm.persistence import save_sla_violation, update_task
from cloudrm.runtime import ServiceRuntime

configure_logging()
logger = logging.getLogger("sla-agent")

runtime = ServiceRuntime("sla-agent", ["request.classified", "execution.done", "execution.failed"])
worker_task: asyncio.Task[None] | None = None


def risk_severity(risk: float) -> str:
    if risk >= 0.95:
        return "critical"
    if risk >= 0.8:
        return "high"
    if risk >= 0.5:
        return "medium"
    return "low"


def assess_risk(task: dict[str, Any]) -> dict[str, Any]:
    queue_length = int(task.get("queue_length", 0))
    duration = float(task.get("duration_seconds", 1))
    deadline = float(task.get("sla_deadline_seconds", 30))
    estimated_wait = float(task.get("estimated_wait", queue_length * 0.5))
    risk = min(1.0, max(0.0, (estimated_wait + duration) / max(deadline, 0.001)))
    severity = risk_severity(risk)
    recommendation = "scale" if severity == "critical" else "boost" if severity in {"high", "medium"} else "observe"
    return {
        "estimated_wait": round(estimated_wait, 4),
        "deadline": deadline,
        "risk": round(risk, 4),
        "severity": severity,
        "recommendation": recommendation,
        "unsafe": severity == "critical",
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def actual_sla_violation(task: dict[str, Any], now: datetime | None = None) -> dict[str, Any] | None:
    deadline = float(task.get("sla_deadline_seconds", 30))
    current = now or datetime.now(UTC)
    created_at = _parse_dt(task.get("created_at")) or _parse_dt(task.get("created_at_iso"))
    started_at = _parse_dt(task.get("started_at"))
    finished_at = _parse_dt(task.get("finished_at"))
    wait_seconds = None
    execution_seconds = None
    if created_at is not None:
        wait_end = started_at or current
        wait_seconds = max(0.0, (wait_end - created_at).total_seconds())
    if started_at is not None:
        exec_end = finished_at or current
        execution_seconds = max(0.0, (exec_end - started_at).total_seconds())
    exceeded_wait = wait_seconds is not None and wait_seconds > deadline
    exceeded_execution = execution_seconds is not None and execution_seconds > deadline
    if not exceeded_wait and not exceeded_execution:
        return None
    severity = "critical" if max(wait_seconds or 0.0, execution_seconds or 0.0) > deadline * 2 else "high"
    return {
        "deadline": deadline,
        "wait_seconds": wait_seconds,
        "execution_seconds": execution_seconds,
        "severity": severity,
    }


async def handle_classified(envelope: EventEnvelope) -> None:
    assert runtime.redis is not None
    assert runtime.db is not None
    task = envelope.payload
    assessment = assess_risk(task)
    severity = assessment["severity"]
    if severity != "low":
        SLA_RISKS.labels(severity).inc()
    boosted_priority = min(10, int(task.get("dynamic_priority", task.get("priority", 1))) + (2 if severity in {"high", "critical"} else 1 if severity == "medium" else 0))
    if boosted_priority != int(task.get("dynamic_priority", task.get("priority", 1))):
        await update_task(runtime.db, task["task_id"], priority=boosted_priority)
    await runtime.redis.hset(
        f"sla:{task['task_id']}",
        mapping={
            "risk": str(assessment["risk"]),
            "severity": severity,
            "unsafe": str(assessment["unsafe"]).lower(),
            "recommendation": assessment["recommendation"],
        },
    )
    payload = {
        **task,
        **assessment,
        "sla_risk": assessment["risk"],
        "sla_severity": severity,
        "boosted_priority": boosted_priority,
    }
    await runtime.publish(
        "sla_risk",
        EventEnvelope(event_type="sla.risk", correlation_id=envelope.correlation_id, source="sla-agent", payload=payload),
    )
    if severity in {"medium", "high", "critical"}:
        await runtime.publish(
            "sla_boost",
            EventEnvelope(event_type="sla.boost", correlation_id=envelope.correlation_id, source="sla-agent", payload=payload),
        )


async def handle_execution(envelope: EventEnvelope) -> None:
    assert runtime.db is not None
    violation = actual_sla_violation(envelope.payload)
    if violation is None:
        return
    severity = violation["severity"]
    SLA_VIOLATIONS.labels(severity).inc()
    await save_sla_violation(
        runtime.db,
        task_id=envelope.payload["task_id"],
        correlation_id=envelope.correlation_id,
        severity=severity,
        details={**violation, "event_type": envelope.event_type},
    )


async def worker() -> None:
    assert runtime.consumer is not None
    async for message, envelope in runtime.consumer.events():
        started = time.perf_counter()
        try:
            if envelope.event_type == "request.classified":
                await handle_classified(envelope)
            elif envelope.event_type in {"execution.done", "execution.failed"}:
                await handle_execution(envelope)
            await runtime.consumer.commit()
            runtime.last_error = None
        except Exception as exc:  # noqa: BLE001
            runtime.last_error = str(exc)
            logger.exception("SLA event processing failed")
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
        return {"status": "ok", "service": "sla-agent", "ready": True}
    return JSONResponse(status_code=503, content={"status": "not_ready", "service": "sla-agent", "ready": False})


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

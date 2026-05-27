from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from cloudrm.messages import EventEnvelope


async def record_event(engine: AsyncEngine, envelope: EventEnvelope) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO events(event_id, correlation_id, event_type, source, payload, occurred_at)
                VALUES (:event_id, :correlation_id, :event_type, :source, CAST(:payload AS JSONB), :occurred_at)
                ON CONFLICT (event_id) DO NOTHING
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


async def update_task(engine: AsyncEngine, task_id: str, **fields: Any) -> None:
    if not fields:
        return
    allowed = {
        "status",
        "priority",
        "assigned_node_id",
        "error",
    }
    values = {key: value for key, value in fields.items() if key in allowed}
    if not values:
        return
    values["updated_at"] = datetime.now(UTC)
    values["task_id"] = task_id
    assignments = ", ".join(f"{key} = :{key}" for key in values if key != "task_id")
    async with engine.begin() as connection:
        await connection.execute(text(f"UPDATE tasks SET {assignments} WHERE task_id = :task_id"), values)


async def get_task(engine: AsyncEngine, task_id: str) -> dict[str, Any] | None:
    async with engine.connect() as connection:
        result = await connection.execute(text("SELECT * FROM tasks WHERE task_id = :task_id"), {"task_id": task_id})
        row = result.mappings().first()
    return dict(row) if row else None


async def save_decision(
    engine: AsyncEngine,
    *,
    task_id: str,
    correlation_id: str,
    node_id: str | None,
    utility: float,
    decision_type: str,
    payload: dict[str, Any],
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO decisions(decision_id, task_id, correlation_id, node_id, utility, decision_type, payload)
                VALUES (:decision_id, :task_id, :correlation_id, :node_id, :utility, :decision_type, CAST(:payload AS JSONB))
                """
            ),
            {
                "decision_id": str(uuid4()),
                "task_id": task_id,
                "correlation_id": correlation_id,
                "node_id": node_id,
                "utility": utility,
                "decision_type": decision_type,
                "payload": json.dumps(payload, ensure_ascii=False),
            },
        )


async def save_execution(
    engine: AsyncEngine,
    *,
    task_id: str,
    correlation_id: str,
    node_id: str | None,
    status: str,
    started_at: datetime | None,
    finished_at: datetime | None,
    details: dict[str, Any],
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO execution_history(
                    execution_id, task_id, correlation_id, node_id, status, started_at, finished_at, details
                )
                VALUES (
                    :execution_id, :task_id, :correlation_id, :node_id, :status, :started_at, :finished_at,
                    CAST(:details AS JSONB)
                )
                """
            ),
            {
                "execution_id": str(uuid4()),
                "task_id": task_id,
                "correlation_id": correlation_id,
                "node_id": node_id,
                "status": status,
                "started_at": started_at,
                "finished_at": finished_at,
                "details": json.dumps(details, ensure_ascii=False),
            },
        )


async def save_sla_violation(
    engine: AsyncEngine,
    *,
    task_id: str,
    correlation_id: str,
    severity: str,
    details: dict[str, Any],
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                """
                INSERT INTO sla_violations(violation_id, task_id, correlation_id, severity, details)
                VALUES (:violation_id, :task_id, :correlation_id, :severity, CAST(:details AS JSONB))
                """
            ),
            {
                "violation_id": str(uuid4()),
                "task_id": task_id,
                "correlation_id": correlation_id,
                "severity": severity,
                "details": json.dumps(details, ensure_ascii=False),
            },
        )

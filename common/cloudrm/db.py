from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_engine(dsn: str) -> AsyncEngine:
    return create_async_engine(dsn, pool_pre_ping=True, future=True)


async def run_migrations(engine: AsyncEngine) -> None:
    statements = [
        """
        CREATE TABLE IF NOT EXISTS tasks (
            task_id UUID PRIMARY KEY,
            correlation_id UUID NOT NULL,
            status TEXT NOT NULL,
            task_type TEXT NOT NULL,
            priority INTEGER NOT NULL,
            cpu_required DOUBLE PRECISION NOT NULL,
            ram_required_mb INTEGER NOT NULL,
            duration_seconds DOUBLE PRECISION NOT NULL,
            sla_deadline_seconds INTEGER NOT NULL,
            requires_gpu BOOLEAN NOT NULL DEFAULT FALSE,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            assigned_node_id TEXT,
            error TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS events (
            event_id UUID PRIMARY KEY,
            correlation_id UUID NOT NULL,
            event_type TEXT NOT NULL,
            source TEXT NOT NULL,
            payload JSONB NOT NULL,
            occurred_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS decisions (
            decision_id UUID PRIMARY KEY,
            task_id UUID NOT NULL,
            correlation_id UUID NOT NULL,
            node_id TEXT,
            utility DOUBLE PRECISION NOT NULL,
            decision_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS execution_history (
            execution_id UUID PRIMARY KEY,
            task_id UUID NOT NULL,
            correlation_id UUID NOT NULL,
            node_id TEXT,
            status TEXT NOT NULL,
            started_at TIMESTAMPTZ,
            finished_at TIMESTAMPTZ,
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS sla_violations (
            violation_id UUID PRIMARY KEY,
            task_id UUID NOT NULL,
            correlation_id UUID NOT NULL,
            severity TEXT NOT NULL,
            details JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)",
        "CREATE INDEX IF NOT EXISTS idx_events_correlation_id ON events(correlation_id)",
    ]
    async with engine.begin() as connection:
        for statement in statements:
            await connection.execute(text(statement))


async def ping_db(engine: AsyncEngine) -> bool:
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
    return True

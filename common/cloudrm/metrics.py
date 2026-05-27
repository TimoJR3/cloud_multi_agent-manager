from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

API_REQUESTS = Counter("cloudrm_api_requests_total", "Total API HTTP requests", ["method", "path"])
TASKS_SUBMITTED = Counter("cloudrm_tasks_submitted_total", "Submitted tasks")
KAFKA_EVENTS = Counter("cloudrm_kafka_events_total", "Published Kafka events", ["topic", "source"])
RABBITMQ_EVENTS = Counter("cloudrm_rabbitmq_events_total", "Published RabbitMQ events", ["routing_key", "source"])

QUEUE_LENGTH = Gauge("cloudrm_queue_length", "Task queue length", ["class"])
NODE_UTILIZATION = Gauge("cloudrm_node_utilization_ratio", "Node utilization ratio", ["node_id", "resource"])

SLA_RISKS = Counter("cloudrm_sla_risks_total", "Detected SLA risks", ["severity"])
SLA_VIOLATIONS = Counter("cloudrm_sla_violations_total", "Actual SLA violations", ["severity"])

AGENT_LATENCY = Histogram("cloudrm_agent_latency_seconds", "Agent processing latency", ["agent"])
REQUEST_WAIT_SECONDS = Histogram("cloudrm_request_wait_seconds", "Actual request queue wait seconds", ["class"])
DECISION_LATENCY_SECONDS = Histogram("cloudrm_decision_latency_seconds", "Coordinator decision latency seconds")

EXECUTION_FAILURES = Counter("cloudrm_execution_failures_total", "Execution failures", ["reason"])
CONSUMED_MESSAGES = Counter(
    "cloudrm_consumed_messages_total",
    "Consumed broker messages",
    ["backend", "queue", "event_type", "source"],
)
DEAD_LETTER_MESSAGES = Counter(
    "cloudrm_dead_letter_messages_total",
    "Messages sent to dead-letter handling",
    ["queue", "reason"],
)

SCALING_ACTIONS = Counter("cloudrm_scaling_actions_total", "Scaling actions", ["action", "status"])
SCALING_COOLDOWN_BLOCKS = Counter("cloudrm_scaling_cooldown_blocks_total", "Scaling actions blocked by cooldown")

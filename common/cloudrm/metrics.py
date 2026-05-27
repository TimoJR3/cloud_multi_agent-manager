from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

API_REQUESTS = Counter("cloudrm_api_requests_total", "Количество HTTP-запросов API", ["method", "path"])
TASKS_SUBMITTED = Counter("cloudrm_tasks_submitted_total", "Количество созданных задач")
KAFKA_EVENTS = Counter("cloudrm_kafka_events_total", "Количество опубликованных Kafka-событий", ["topic", "source"])
RABBITMQ_EVENTS = Counter("cloudrm_rabbitmq_events_total", "Количество опубликованных RabbitMQ-сообщений", ["routing_key", "source"])
QUEUE_LENGTH = Gauge("cloudrm_queue_length", "Длина очереди задач", ["queue"])
NODE_UTILIZATION = Gauge("cloudrm_node_utilization_ratio", "Утилизация узла", ["node_id", "resource"])
SLA_VIOLATIONS = Counter("cloudrm_sla_violations_total", "Количество нарушений SLA")
AGENT_LATENCY = Histogram("cloudrm_agent_latency_seconds", "Задержка обработки агента", ["agent"])

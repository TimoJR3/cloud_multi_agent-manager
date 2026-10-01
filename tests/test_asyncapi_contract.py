"""docs/asyncapi.yaml must stay in sync with the broker topology and config in code."""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest
import yaml

from cloudrm.rabbit import RabbitMQSettings

ROOT = Path(__file__).resolve().parents[1]
SPEC = yaml.safe_load((ROOT / "docs" / "asyncapi.yaml").read_text(encoding="utf-8"))
CONFIG = yaml.safe_load((ROOT / "config" / "services.yaml").read_text(encoding="utf-8"))

EVENT_TYPES = set(CONFIG["kafka"]["topics"].values())
CHANNELS = SPEC["channels"]
MESSAGES = SPEC["components"]["messages"]


def _ref_name(ref: str) -> str:
    return ref.rsplit("/", 1)[-1]


def _channel_by_address(address: str) -> dict:
    return next(channel for channel in CHANNELS.values() if channel["address"] == address)


def _topic_matches(pattern: str, routing_key: str) -> bool:
    regex = re.escape(pattern).replace(r"\*", r"[^.]+").replace(r"\#", r".*")
    return re.fullmatch(regex, routing_key) is not None


def test_spec_has_one_channel_per_configured_topic() -> None:
    event_channels = {
        channel["address"]
        for channel in CHANNELS.values()
        if channel.get("bindings", {}).get("$ref", "").endswith("/eventsExchange")
    }
    assert event_channels == EVENT_TYPES


def test_event_channel_messages_declare_matching_event_type() -> None:
    for event_type in EVENT_TYPES:
        channel = _channel_by_address(event_type)
        (message_ref,) = [message["$ref"] for message in channel["messages"].values()]
        message = MESSAGES[_ref_name(message_ref)]
        assert message["name"] == event_type
        declared = message["payload"]["allOf"][1]["properties"]["event_type"]["const"]
        assert declared == event_type


@pytest.mark.parametrize("queue_name", sorted(RabbitMQSettings().queues))
def test_queue_channel_lists_exactly_the_events_routed_to_it(queue_name: str) -> None:
    bindings = RabbitMQSettings().queues[queue_name]
    routed = {event for event in EVENT_TYPES if any(_topic_matches(pattern, event) for pattern in bindings)}
    channel = _channel_by_address(queue_name)
    listed = {MESSAGES[_ref_name(message["$ref"])]["name"] for message in channel["messages"].values()}
    assert listed == routed


def test_reliability_topology_matches_rabbit_settings() -> None:
    settings = RabbitMQSettings()
    exchange = SPEC["components"]["channelBindings"]["eventsExchange"]["amqp"]["exchange"]
    assert exchange["name"] == settings.exchange
    assert CHANNELS["retryExchange"]["bindings"]["amqp"]["exchange"]["name"] == settings.retry_exchange
    assert CHANNELS["deadLetterQueue"]["address"] == "queue.dead"
    publish = SPEC["components"]["operationBindings"]["amqpPublish"]["amqp"]
    assert publish["expiration"] == settings.default_ttl_seconds * 1000


@pytest.mark.parametrize(
    ("operation_id", "module", "service"),
    [
        ("queueAgentConsumeKafka", "services.queue_agent.main", "queue-agent"),
        ("resourceAgentConsumeKafka", "services.resource_agent.main", "resource-agent"),
        ("slaAgentConsumeKafka", "services.sla_agent.main", "sla-agent"),
        ("forecastAgentConsumeKafka", "services.forecast_agent.main", "forecast-agent"),
        ("coordinatorConsumeKafka", "services.coordinator_agent.main", "coordinator-agent"),
        ("executorConsumeKafka", "services.executor_agent.main", "executor-agent"),
        ("scaleAgentConsumeKafka", "services.scale_agent.main", "scale-agent"),
    ],
)
def test_kafka_consumers_match_service_subscriptions(operation_id: str, module: str, service: str) -> None:
    runtime = importlib.import_module(module).runtime
    operation = SPEC["operations"][operation_id]
    channel = CHANNELS[_ref_name(operation["channel"]["$ref"])]
    binding = SPEC["components"]["operationBindings"][_ref_name(operation["bindings"]["$ref"])]["kafka"]

    assert runtime.service_name == service
    assert binding["groupId"]["const"] == service
    assert channel["address"] in runtime.topics

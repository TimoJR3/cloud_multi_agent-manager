from cloudrm.rabbit import RabbitMQSettings


def test_rabbit_settings_from_config() -> None:
    settings = RabbitMQSettings.from_config(
        {
            "rabbitmq": {
                "host": "rabbitmq",
                "port": "5672",
                "username": "mas",
                "password": "secret",
                "exchange": "mas.events",
                "dlx": "mas.dlx",
                "retry_exchange": "mas.retry",
                "prefetch": "10",
            }
        }
    )

    assert settings.amqp_url == "amqp://mas:secret@rabbitmq:5672/"
    assert settings.exchange == "mas.events"
    assert settings.dlx == "mas.dlx"
    assert settings.retry_exchange == "mas.retry"
    assert settings.prefetch == 10


def test_rabbit_default_topology_contains_required_bindings() -> None:
    settings = RabbitMQSettings()

    assert settings.queues["queue.requests"] == ["request.created", "sla.boost", "sla.risk"]
    assert settings.queues["queue.resources"] == ["need_placement", "node.metrics.*", "node.unavailable"]
    assert "node.proposal.*" in settings.queues["queue.coordinator"]
    assert settings.queues["queue.executor"] == ["decision.dispatch", "node.unavailable"]
    assert settings.queues["queue.scale"] == ["decision.scale"]


def test_rabbit_service_queue_mapping() -> None:
    settings = RabbitMQSettings()

    assert settings.queue_for_service("queue-agent") == "queue.requests"
    assert settings.queue_for_service("scale-agent") == "queue.scale"
    assert settings.queue_for_service("unknown") is None

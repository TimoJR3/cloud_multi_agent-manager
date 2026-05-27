# CloudRM: мультиагентное управление очередями и ресурсами облачного ЦОД

Исследовательский прототип для ВКР «Разработка мультиагентной системы для управления очередями и ресурсами в облачных ЦОД». Система моделирует событийный контур: API принимает заявки, queue-agent классифицирует их и ведет очереди, resource-agent формирует предложения размещения, SLA-agent оценивает риск и фактические нарушения, forecast-agent дает краткосрочный прогноз, coordinator-agent принимает объяснимое решение, executor-agent эмулирует исполнение, scale-agent эмулирует scale-out/scale-in.

RabbitMQ теперь является основным runtime broker для агентного контура. Kafka сохранена как опциональная audit/telemetry шина через `EVENT_BACKEND=kafka|dual`.

## Быстрый Запуск

```bash
make init
docker compose up --build -d
docker compose ps
```

Проверки после запуска:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/ready
python scripts/validate_brokers.py
python scripts/validate_runtime.py
RUN_INTEGRATION=1 pytest -q tests/integration
```

Локальные unit/smoke тесты:

```bash
py -3.11 -m pytest -q
```

## Конфигурация Брокера

`EVENT_BACKEND` задает режим событий:

- `rabbitmq` - основной режим по умолчанию, агенты потребляют RabbitMQ queues.
- `kafka` - совместимый Kafka-only режим.
- `dual` - publish в RabbitMQ и Kafka, consumption через RabbitMQ для агентов.

RabbitMQ topology:

- exchange: `mas.events`
- retry exchange: `mas.retry`
- DLX: `mas.dlx`
- queues: `queue.requests`, `queue.resources`, `queue.sla`, `queue.forecast`, `queue.coordinator`, `queue.executor`, `queue.scale`, `queue.dead`, `queue.retry`

## Сервисы

| Сервис | Порт | Роль |
| --- | ---: | --- |
| `api-service` | 8000 | прием задач, статусы, эксперименты, отказ узла |
| `queue-agent` | 8011 | классовые очереди Redis, aging, SLA boost |
| `resource-agent` | 8012 | CPU/RAM/GPU scoring и `node.proposal.fit` |
| `sla-agent` | 8013 | `sla.risk`, `sla.boost`, фактические `sla_violations` |
| `forecast-agent` | 8014 | moving-average прогноз очереди |
| `coordinator-agent` | 8015 | decision window, utility, fallback dispatch/scale |
| `executor-agent` | 8016 | emulator execution, node failure handling |
| `load-generator` | 8017 | генератор нагрузки |
| `scale-agent` | 8018 | emulator autoscaling по `decision.scale` |
| `prometheus` | 9090 | метрики |
| `grafana` | 3000 | dashboard |

## Основные Сценарии

Создать задачу:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"batch","cpu_required":2,"ram_required_mb":1024,"duration_seconds":3,"priority":4,"sla_deadline_seconds":20}'
```

GPU-задача:

```bash
curl -X POST http://localhost:8000/tasks \
  -H "Content-Type: application/json" \
  -d '{"task_type":"gpu","cpu_required":2,"ram_required_mb":4096,"duration_seconds":3,"priority":5,"sla_deadline_seconds":20,"requires_gpu":true}'
```

Отказ узла:

```bash
curl -X POST http://localhost:8000/nodes/node-a/failure
```

## Эксперименты

Скрипт запускает воспроизводимые сценарии `normal`, `overload`, `gpu_shortage`, `node_failure` в режимах `baseline` и `mas`, затем пишет:

- `artifacts/experiments/results.csv`
- `artifacts/experiments/summary.md`

Команда:

```bash
py -3.11 scripts/run_experiments.py --duration-seconds 30 --seed 42
```

Метрики summary считаются по PostgreSQL при доступности `DATABASE_URL`; если БД недоступна с хоста, скрипт использует только факты, собранные через API, и не подставляет синтетические значения.

## Метрики

Добавлены метрики:

- `cloudrm_request_wait_seconds`
- `cloudrm_decision_latency_seconds`
- `cloudrm_execution_failures_total`
- `cloudrm_consumed_messages_total`
- `cloudrm_dead_letter_messages_total`
- `cloudrm_sla_risks_total`
- `cloudrm_sla_violations_total`
- `cloudrm_scaling_actions_total`
- `cloudrm_scaling_cooldown_blocks_total`
- `cloudrm_queue_length{class=...}`

## Ограничения

Это исследовательский emulator, а не Kubernetes operator. `scale-agent` добавляет виртуальные Redis-узлы `node-auto-N`; future Kubernetes adapter можно подключить за тем же контуром `decision.scale -> scaling.*`. Forecast остается moving-average без тяжелой ML-модели. Авторизация API намеренно не добавлена.

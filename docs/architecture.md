# Architecture

## Overview

Multi-user chat backend with **DDD**, **CQRS**, and event-driven architecture.

### Key Technologies

| Layer | Technology |
|-------|-----------|
| API/WebSocket | FastAPI + ASGI |
| Database | MongoDB + Motor (async driver) |
| Message bus | Apache Kafka |
| DI container | Punq |
| ORM/Access | SQLAlchemy 2.0 |
| Logging | Loguru |

### Architecture Patterns

| Pattern | Location |
|---------|----------|
| CQRS + Mediator | `app/logic/mediator/base.py` |
| Rich domain model | `app/domain/entities/messages.py` |
| Dependency injection | `app/logic/init.py` (`@lru_cache(1)`) |
| Flat import aliases | `application`, `domain`, `infrastructure`, `logic`, `settings`, `tests` |

---

## Directory Structure

```
app/
├── application/   # HTTP/WebSocket API (FastAPI routers, schemas, lifespan)
├── domain/        # Entities, value objects, domain events, exceptions
├── logic/         # Commands, queries, event handlers, Mediator, DI container
├── infrastructure/ # MongoDB repos, Kafka broker, WebSocket manager, adapters
├── settings/      # Environment-based configuration (pydantic-settings)
└── test/          # Unit and API tests (in-memory repos for isolation)
```

---

## Core Flow: Create Message

```
POST /chat/{id}/messages
  ↓
application/api/messages/handlers.py
  ↓
mediator.handle_command(CreateMessageCommand)
  ↓
CreateMessageCommandHandler
  (validates chat exists, builds Message, saves via messages_repository)
  ↓
mediator.publish(chat.pull_events())
  ↓
NewMessageReceivedEventHandler (Kafka + WebSocket publish)
  ↓
response
```

---

## Service URLs

After starting with `make all`:

| Service | URL |
|---------|-----|
| API docs | `http://localhost:8000/api/docs` |
| Mongo Express | `:28081` |
| Kafka UI | `:8090` |
| Prometheus metrics | `:9090` |

---

## Makefile Targets

| Target | Action |
|--------|--------|
| `make all` | Bring up storages + app + Kafka |
| `make storages` | MongoDB + Postgres only |
| `make kafka` | Kafka only |
| `make app` | App + services + Kafka |
| `make all-down` | Shutdown all services |
| `make app-shell` | Terminal for the app |
| `make app-logs` | Tail app logs |

---

## Observability

### The `/metrics` endpoint

The app exposes Prometheus metrics at **`GET /metrics`** on the API port (see
`app/application/api/main.py`):

```bash
curl http://localhost:8000/metrics
```

`prometheus-fastapi-instrumentator` provides the default HTTP metrics
(`http_requests_total`, `http_request_duration_seconds`), and the app's own
metrics are defined **only** in `app/infrastructure/metrics.py` — the single
metric registry (ADR-0006). Prometheus (`prometheus.yml` at the repo root)
scrapes `main-app:${API_PORT}/metrics`; bring it up with `make prometheus`.

### Metric registry

Every metric below is defined in `app/infrastructure/metrics.py`; the table is
kept in sync with the ownership table at the bottom of that file. This is the
final registry table from ADR-0006 (Chunk 7.3): names, types, labels, owners,
meaning, and alert-worthiness.

**Alert-worthiness legend:** ✅ **alerted** — a rule exists in
`docker_compose/prometheus-alerts.yml` (Chunk 7.2); **candidate** — worth
alerting on once an operational baseline exists (ADR-0006 Q1), no rule yet;
**no** — informational/volume signal, alerting on it would be noisy.

| Metric | Type | Labels | Emitted by | Meaning | Alert-worthiness |
|---|---|---|---|---|---|
| `http_requests_total` | Counter | handler, method, status | instrumentator (default) | Total HTTP requests handled, per route/method/status | candidate — 5xx rate (`status=~"5.."`) for RED error alerting |
| `http_request_duration_seconds` | Histogram | handler, method, status | instrumentator (default) | HTTP request latency distribution | candidate — p95 latency |
| `outbox_published_total` | Counter | — | outbox relay (`outbox/relay.py`) | Outbox rows successfully published to Kafka | no — a stall surfaces in `outbox_pending` |
| `outbox_publish_errors_total` | Counter | — | outbox relay (`outbox/relay.py`) | Outbox rows that failed to publish (retried next tick) | ✅ alerted — `OutboxRelayFailing` |
| `outbox_pending` | Gauge | — | outbox relay (`outbox/relay.py`) | Unsent outbox rows — true backlog (not capped by batch size) | ✅ alerted — `OutboxBacklogGrowing` (threshold is the ADR Q1 placeholder) |
| `kafka_messages_sent_total` | Counter | topic | outbox relay (`outbox/relay.py`) | Messages handed to the Kafka producer, per topic | no — volume/attribution |
| `outbox_publish_duration_seconds` | Histogram | topic | outbox relay (`outbox/relay.py`) | Per-row send latency, including failed attempts (timeout latency during a Kafka outage) | candidate — p95 per topic |
| `kafka_messages_consumed_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Broker messages received (counted before parsing) | no — the consumed-vs-published gap is the health signal |
| `kafka_consumer_events_published_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Events successfully published to the mediator | no — read together with consumed |
| `kafka_consumer_errors_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Unexpected exceptions while processing consumed messages (loop survives) | candidate — rate > 0 (pair with malformed) |
| `kafka_consumer_malformed_total` | Counter | — | consumer loop (`api/lifespan.py`) | Consumed messages failing validation (missing `chat_oid`/`message`) | candidate — sustained rate > 0 means a producer/schema bug |
| `kafka_consumer_up` | Gauge | — | consumer lifecycle (`api/lifespan.py`) | 1 while the consumer loop task runs (stays 1 through reconnect backoff); 0 after graceful stop or any non-cancelled task exit | ✅ alerted — `KafkaConsumerDown` (guarded by the app's `up`) |
| `kafka_consumer_reconnects_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Reconnection attempts after the broker stream died or exited cleanly (O-1 reconnect loop with exponential backoff) | ✅ alerted — `KafkaConsumerReconnecting` |
| `circuit_breaker_state` | Gauge | name | circuit breaker (`infrastructure/resilience.py`) | 1 while a guarded dependency's breaker is open or half-open, 0 when closed (`name='mongo'` persistence path, `name='kafka'` outbox relay sends) | candidate — instantaneous signal; the rejection counter below is the recency signal |
| `circuit_breaker_rejected_total` | Counter | name | circuit breaker (`infrastructure/resilience.py`) | Calls rejected fail-fast because a breaker was open (gate rejections only — the failures that trip the breaker are counted by the guarded component, e.g. `outbox_publish_errors_total`) | ✅ alerted — `OutboxRelayCircuitOpen` (`name='kafka'`) |
| `ws_connections_active` | Gauge | — | WS manager (`websockets/managers.py`) | Live WebSocket connections across all chats (recomputed from the manager map, cannot drift) | no — capacity trend |
| `ws_connections_accepted_total` | Counter | — | WS manager (`websockets/managers.py`) | Connections accepted | no |
| `ws_connections_removed_total` | Counter | — | WS manager (`websockets/managers.py`) | Connections removed (counted only when a socket was actually registered) | no — churn vs accepted |
| `ws_messages_broadcast_total` | Counter | — | WS manager (`websockets/managers.py`) | Completed fan-out calls (one per `send_all`) | no |
| `ws_broadcast_failures_total` | Counter | — | WS manager (`websockets/managers.py`) | Per-socket send failures during fan-out (one dead socket no longer aborts delivery) | ✅ alerted — `WSBroadcastFailures` |
| `ws_broadcast_duration_seconds` | Histogram | — | WS manager (`websockets/managers.py`) | Fan-out latency, including failed per-socket attempts | candidate — p95 |
| `mediator_events_published_total` | Counter | event | mediator (`logic/mediator/base.py`) | Events dispatched by `publish`, per event class (closed label set) | no — flow volume |
| `mediator_commands_handled_total` | Counter | command | mediator (`logic/mediator/base.py`) | Commands dispatched to registered handlers (unregistered commands raise and are not counted) | no |
| `mediator_queries_handled_total` | Counter | query | mediator (`logic/mediator/base.py`) | Queries dispatched (counted before the handler call — domain errors still count as dispatched) | no |
| `db_operation_errors_total` | Counter | operation, collection, exception | command/query handlers (`logic/commands\|queries/messages.py`) | Repository (persistence) call exceptions; domain errors (`ChatNotFoundException`, duplicate title) excluded | candidate — rate > 0; strong candidate for a future rule |
| `telegram_notifications_sent_total` | Counter | — | Telegram handler (`logic/events/messages.py`) | Successful notification delivery attempts | no — failed is the signal |
| `telegram_notifications_failed_total` | Counter | — | Telegram handler (`logic/events/messages.py`) | Attempts that raised (logged and swallowed; nothing counted when Telegram is unconfigured) | candidate — rate > 0 |
| `application_info` | Gauge | version | app factory (`application/api/main.py`) | Build info: constant 1, the deployed version is the label (`APP_VERSION` env) | no — deployment tracking |

> Planning new metrics? Follow `docs/adr/0006-metrics-implementation-plan.md`
> (Section 3.2) and add them to the registry module — never define ad-hoc
> metrics in feature modules. All metric updates in business code go through
> the `safe_*` helpers in the registry (D5: metrics are non-fatal).

### Alert rules (G7)

Prometheus evaluates the rules in `docker_compose/prometheus-alerts.yml`
(loaded via `rule_files` in `prometheus.yml`, mounted read-only by
`docker_compose/prometheus.yaml`, evaluated every 15s). Group `kafka-chat`:

| Alert | Expression | Severity | Fires when |
|---|---|---|---|
| `OutboxBacklogGrowing` | `outbox_pending > 200 for 5m` | warning | the relay stops draining the outbox (Kafka outage, dead relay, producers outpacing it) |
| `OutboxRelayFailing` | `rate(outbox_publish_errors_total[5m]) > 0` | warning | the relay recorded publish errors in the last 5m — rows stay unsent and retry |
| `OutboxRelayCircuitOpen` | `increase(circuit_breaker_rejected_total{name='kafka'}[15m]) > 0` | warning | the relay's 'kafka' circuit breaker is rejecting sends — outbox batches are skipped while the breaker stays open (which also quiets `OutboxRelayFailing`); rows drain once the half-open probe succeeds |
| `KafkaConsumerDown` | `kafka_consumer_up == 0 and up{job='kafka-chat-api'} == 1` | critical | the consumer loop task is dead while the app itself is up — inbound messages stop reaching the fan-out |
| `KafkaConsumerReconnecting` | `increase(kafka_consumer_reconnects_total[15m]) > 0` | warning | at least one consumer reconnect in the last 15m — the broker stream died or exited cleanly; delivery self-heals via backoff, repeated firing signals Kafka instability |
| `WSBroadcastFailures` | `rate(ws_broadcast_failures_total[5m]) > 0` | warning | per-socket send failures during fan-out (clients dropping mid-broadcast) |

The `outbox_pending > 200` threshold is calibrated from the 2026-09-17 Locust
baseline (50 users, ~16 msg/s: `outbox_pending` max=35, p95=21; relay drained to
0 with 0 errors — see ADR-0006 Q1). Recalibrate after major load-profile changes
(`loadtest/locustfile.py`). Alertmanager routing/wiring is a deferred follow-up:
alerts currently terminate in Prometheus's own UI. Validate rules with
`promtool check rules` before merging rule changes.

Metrics marked **candidate** in the registry above are the pool for future
rules (HTTP 5xx rate, p95 latencies, consumer errors/malformed, DB failures,
Telegram failures) — see the alert-worthiness column.

---

## Testing

```bash
# From app/ directory
cd app && poetry run pytest

# Lint/format check
poetry run ruff check .
poetry run pre-commit run --all-files
```

---

## Before Modifying

1. Read `app/logic/init.py` — all wiring happens there.
2. Use `base`/`mongo`/`memory` repository split — never import Motor in domain/logic.
3. Keep `domain/` entities free of infrastructure imports.
4. Check `docs/known-issues.md` for latent bugs.

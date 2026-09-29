# Architecture

## Overview

Multi-user chat backend with **DDD**, **CQRS**, and event-driven architecture.

### Key Technologies

| Layer | Technology |
|-------|-----------|
| API/WebSocket | FastAPI + ASGI |
| Database | MongoDB + Motor (async driver) |
| Message bus | Apache Kafka |
| Cache / ephemeral state | Valkey (ADR-0008 — cache-aside, chat presence, relay leader lock) |
| DI container | Punq |
| Logging | stdlib logging → JSON lines (`infrastructure/logging_config.py`, parsed by Promtail) |

(Full stack incl. tooling and versions: README → "Technology Stack".)

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
├── infrastructure/ # MongoDB repos, Kafka broker, Valkey client, WS manager, adapters
│                   # (cache/, presence/, locks/ — all behind ABCs, see ADR-0008)
├── settings/      # Environment-based configuration (pydantic-settings)
└── test/          # Unit and API tests (in-memory repos for isolation)
```

---

## Core Flow: Create Message

```
POST /chat/{id}/messages
  ↓
application/api/messages/handlers.py → mediator.handle_command(CreateMessageCommand)
  ↓
CreateMessageCommandHandler
  (validates chat exists, builds Message, saves the message AND the outbox row
   in ONE Mongo transaction, then publishes the in-process domain events)
  ↓
response
```

Background tasks started in the app lifespan:

- **Outbox relay** (`infrastructure/outbox/relay.py`) polls unsent outbox rows, publishes each
  to its Kafka topic, marks it sent — it is the **only** Kafka writer. Sends are guarded by the
  `'kafka'` circuit breaker (open ⇒ the batch is skipped and retried later). With
  `RELAY_LOCK_ENABLED=true` it also holds a Valkey leader lease, so only one replica polls at all.
- **Consumer loop** (`application/api/lifespan.py`) consumes the topic and publishes
  `NewMessageReceivedFromBrokerEvent` → WebSocket fan-out to the chat room.
- **Presence heartbeat** (one per accepted WebSocket, `websockets/managers.py`) refreshes the
  socket's field in the `presence:{chat_oid}` hash every `PRESENCE_TTL_SECONDS / 3`. Cancelled on
  disconnect; a socket that stops beating expires on its own, so an abrupt process death needs no
  cleanup job.

In-process event handlers (`logic/events/messages.py`) keep only side effects like the WebSocket
disconnect on chat deletion — they never send to Kafka directly. Full sequence diagram:
README → "Request flow (create message)".

---

## Valkey: cache, presence, and the relay lock (ADR-0008)

Three optional features, all behind ABCs and all **defaulting off** in `Config()`
(`CACHE_ENABLED`, `PRESENCE_ENABLED`, `RELAY_LOCK_ENABLED`) so the app boots and behaves exactly
as it did before the ADR unless compose turns them on. `infrastructure/cache/valkey.py` is the
only module that imports `valkey`.

| Feature | Where | Notes |
|---|---|---|
| Cache-aside reads | `infrastructure/cache/cached.py` | `CircuitBreaker(Cached(Mongo))` — the cache proxy sits INSIDE the breaker so a Valkey error never counts as a Mongo failure |
| Write-path invalidation | `logic/commands/messages.py` | A post bumps `chat:ver:{oid}`; page keys embed the version, so stale pages die by TTL with no `SCAN` in the hot path. A delete drops the detail + version keys |
| Chat presence | `infrastructure/presence/` | `presence:{chat_oid}` hash, one field per socket, key TTL refreshed per heartbeat |
| Relay leader lock | `infrastructure/locks/` | `SET NX PX` lease with compare-and-extend renew and compare-and-delete release |

**Degradation is the contract.** Every cache call is best-effort: a failure is counted on
`cache_errors_total`, logged, and returned as the value a cold cache would have produced. Reads
fall through to Mongo, writes still succeed, and an open `'valkey'` breaker never surfaces as an
HTTP 503. `noeviction` (128 MiB) keeps the relay lock from being silently evicted. Triage:
`docs/runbooks/valkey-outage.md`.

---

## Service URLs

Listed in README → "Access services" (API docs, Mongo Express, Kafka UI, Prometheus,
Alertmanager, Loki, Grafana — ports from `.env` after `make all`).

---

## Makefile Targets

Listed in README → "Makefile Commands" (up/down/logs per stack, plus `app-shell`). Note the
multi-service targets (`all`, `prometheus`) intentionally merge several compose files so the
services share the `backend` network — keep each target's file list symmetric between `up`
and `down`, or compose warns about orphan containers.

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
| `outbox_pending` | Gauge | — | outbox relay (`outbox/relay.py`) | Unsent outbox rows — true backlog (not capped by batch size) | ✅ alerted — `OutboxBacklogGrowing` (threshold calibrated from the 2026-09-17 Locust baseline) |
| `kafka_messages_sent_total` | Counter | topic | outbox relay (`outbox/relay.py`) | Messages handed to the Kafka producer, per topic | no — volume/attribution |
| `outbox_publish_duration_seconds` | Histogram | topic | outbox relay (`outbox/relay.py`) | Per-row send latency, including failed attempts (timeout latency during a Kafka outage) | candidate — p95 per topic |
| `kafka_messages_consumed_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Broker messages received (counted before parsing) | no — the consumed-vs-published gap is the health signal |
| `kafka_consumer_events_published_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Events successfully published to the mediator | no — read together with consumed |
| `kafka_consumer_errors_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Unexpected exceptions while processing consumed messages (loop survives) | candidate — rate > 0 (pair with malformed) |
| `kafka_consumer_malformed_total` | Counter | — | consumer loop (`api/lifespan.py`) | Consumed messages failing validation (missing `chat_oid`/`message`) | candidate — sustained rate > 0 means a producer/schema bug |
| `kafka_consumer_up` | Gauge | — | consumer lifecycle (`api/lifespan.py`) | 1 while the consumer loop task runs (stays 1 through reconnect backoff); 0 after graceful stop or any non-cancelled task exit | ✅ alerted — `KafkaConsumerDown` (guarded by the app's `up`) |
| `kafka_consumer_reconnects_total` | Counter | topic | consumer loop (`api/lifespan.py`) | Reconnection attempts after the broker stream died or exited cleanly (O-1 reconnect loop with exponential backoff) | ✅ alerted — `KafkaConsumerReconnecting` |
| `circuit_breaker_state` | Gauge | name | circuit breaker (`infrastructure/resilience.py`) | 1 while a guarded dependency's breaker is open or half-open, 0 when closed (`name='mongo'` persistence path, `name='kafka'` outbox relay sends, `name='valkey'` cache client) | candidate — instantaneous signal; the rejection counter below is the recency signal |
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
| `cache_operations_total` | Counter | operation, result | cache proxies (`infrastructure/cache/cached.py`) + Valkey client (`cache/valkey.py`) | Cache operations: `hit`/`miss` for reads, `ok` for writes and invalidations, `error` for failures (no chat oids — cache keys are full of them) | no — volume; derive the hit rate from it |
| `cache_errors_total` | Counter | operation | Valkey client (`infrastructure/cache/valkey.py`) | Real Valkey failures, counted **pre-breaker** (rejections go to `circuit_breaker_rejected_total{name='valkey'}`) | ✅ alerted — `CacheErrorsHigh` |
| `presence_heartbeat_failures_total` | Counter | — | WS manager (`websockets/managers.py`) | Presence heartbeat/refresh failures — a socket stopped refreshing and will be reported as gone once its TTL lapses | candidate — rate > 0 |
| `outbox_relay_lock_acquired_total` | Counter | — | outbox relay lease (`outbox/relay.py`) | Times this process acquired the outbox relay leader lock (ADR-0008) | no — expected on every deploy/restart |
| `outbox_relay_lock_held` | Gauge | — | outbox relay lease (`outbox/relay.py`) | 1 while this process holds the leader lease, 0 otherwise (per process; any replica reporting 1 means a leader exists) | candidate — 0 across all replicas means the relay is not publishing |

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
| `CacheErrorsHigh` | `increase(cache_errors_total[15m]) > 0` | warning | Valkey operation failures — a **degradation, not an outage**: reads fall back to Mongo, writes keep succeeding, presence under-reports and the relay skips its ticks until the lease expires (ADR-0008) |

The `outbox_pending > 200` threshold is calibrated from the 2026-09-17 Locust
baseline (50 users, ~16 msg/s: `outbox_pending` max=35, p95=21; relay drained to
0 with 0 errors — see ADR-0006 Q1). Recalibrate after major load-profile changes
(`loadtest/locustfile.py`). Alerts are delivered to **Alertmanager**
(`docker_compose/alertmanager.yaml`, UI at `:${ALERTMANAGER_PORT}`) per
`docs/adr/0007-alertmanager-wiring.md`: severity-asymmetric routing, inhibition pairs
(CircuitOpen mutes RelayFailing; ConsumerDown mutes Reconnecting). `oncall-critical` and
`team-warnings` deliver **twice**: to **Telegram** (built-in `telegram_configs` receiver; token
and chat id come from gitignored secret files mounted via compose secrets — see
`docs/adr/0007-alertmanager-wiring.md` §4) and to the app's webhook sink `POST /ops/alerts` —
so every alert lands in Telegram AND in the structured JSON logs (Loki/dashboard log panel);
`default-log` is webhook-only. **Every alert carries a
`runbook_url` annotation** (`docs/runbooks/`: `kafka-outage.md` for the outbox/relay pair,
`kafka-consumer.md` for the consumer pair, `ws-fanout.md` for broadcast failures,
`valkey-outage.md` for cache errors), which the sink
appends to the logged message. Validate rules with
`promtool check rules` before merging rule changes.

Metrics marked **candidate** in the registry above are the pool for future
rules (HTTP 5xx rate, p95 latencies, consumer errors/malformed, DB failures,
Telegram failures) — see the alert-worthiness column.

---

## Testing

```bash
# From app/ directory
cd app && uv run pytest

# Lint/format check
uv run ruff check .
uv run pre-commit run --all-files
```

---

## Before Modifying

1. Read `app/logic/init.py` — all wiring happens there.
2. Use `base`/`mongo`/`memory` repository split — never import Motor in domain/logic.
3. Keep `domain/` entities free of infrastructure imports.
4. Check `docs/known-issues.md` for latent bugs.

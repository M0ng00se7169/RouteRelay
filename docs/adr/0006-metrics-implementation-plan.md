# ADR-0006: Metrics & Observability — Analysis and Implementation Plan

**Status**: Proposed (plan only — nothing implemented)
**Created**: 2026-09-10
**Scope**: `app/` (FastAPI + Kafka + MongoDB chat backend), Prometheus stack already provisioned
**Related**: ADR-0001 (WebSocket Kafka relay), `docs/stateless-inventing-yao.md` (outbox + metrics plan), `docs/adr/issue4.md` (Telegram integration), `docs/known-issues.md`

---

## 0. Purpose

This document analyzes **what metrics exist today**, **what is missing**, and proposes a
**step-by-step implementation plan** divided into small, independently verifiable chunks.
Per the request, **no code is changed** — this is a plan only.

### Goals

1. Single source of truth for metric definitions (naming, type, labels, ownership).
2. Close observability gaps: WebSocket fan-out, Kafka consumer lag/health, Telegram
   delivery, Mongo query failures, process/replica-set health.
3. Keep the existing zero-dependency, module-level metric style (no DI for metrics) that
   the codebase and tests already rely on.
4. Make every metric testable with the established `REGISTRY.get_sample_value` pattern.

### Non-goals

- No tracing/OpenTelemetry (could be a future ADR).
- No alerting rules (Alertmanager) — noted as a follow-up.
- No change to the outbox relay semantics (at-least-once stays).

---

## 1. Current State Analysis (verified 2026-09-10)

### 1.1 What already exists ✅

| Concern | Where | Mechanism |
|---|---|---|
| HTTP request metrics | `app/application/api/main.py` | `PrometheusFastApiInstrumentator().instrument(app).expose(app, endpoint='/metrics')` — default handler gives `http_requests_total`, `http_request_duration_seconds` (histogram), latency summaries |
| Outbox relay metrics | `app/infrastructure/metrics.py` | Module-level `Counter`/`Gauge`: `outbox_published_total`, `outbox_publish_errors_total`, `outbox_pending`, `kafka_messages_sent_total` |
| Relay instrumentation | `app/infrastructure/outbox/relay.py` | `_tick()` sets `outbox_pending`, increments publish/error counters |
| Scrape config | `prometheus.yml` (repo root) | job `kafka-chat-api` → `main-app:${API_PORT}` `/metrics`, self-scrape job |
| Prometheus container | `docker_compose/prometheus.yaml` | `prom/prometheus:latest`, port `PROMETHEUS_PORT` (default 9090), mounts `prometheus.yml` |
| Grafana/Loki/Promtail | `docker_compose/observability.yaml` | Grafana 11.1 + Loki 3.1 + Promtail (JSON logs via `configure_json_logging`) |
| Relay metric tests | `app/test/infrastructure/outbox/test_relay.py` | `REGISTRY.get_sample_value(name)` with baseline-delta assertions — the house test pattern |
| Config knob | `app/settings/config.py` | `prometheus_port: int` (Prometheus server, not the app endpoint) |

### 1.2 What is missing ❌ (the gaps this plan closes)

| # | Gap | Why it matters |
|---|---|---|
| G1 | **No WebSocket metrics** — connections, fan-out broadcasts, send failures invisible | WS fan-out is the core UX path (`NewMessageReceivedFromBrokerEventHandler`) |
| G2 | **No inbound Kafka consumer metrics** — consumed count, malformed messages, mediator errors in `_kafka_consumer_loop` | Can't tell if the relay loop (`app/application/api/lifespan.py`) is alive or erroring |
| G3 | **No Telegram notification metrics** — sends, failures are only logged (`logger.warning`) | Silent degradation when misconfigured |
| G4 | **No Mongo/DB failure metrics** — repository exceptions only bubble up as HTTP 400/500 | Can't distinguish DB outages from user error |
| G5 | **No application-info metric** — `application_info` gauge with version | Deployment tracking in dashboards |
| G6 | **No dashboards/provisioning** — Grafana folder `docker_compose/grafana/` is empty | Metrics exist but nothing renders them |
| G7 | **No alert rules** — nothing fires when `outbox_pending` grows unbounded | Outbox backlog is the #1 operational risk |
| G8 | **Metric definitions not documented** — no registry of names/types/labels | Drift risk as more metrics are added |
| G9 | **Kafka per-topic counters missing** — `kafka_messages_sent_total` has no `topic` label | Can't attribute traffic per topic |
| G10 | **`outbox_pending` capped at batch_size** — gauge is set from `get_unsent(self.batch_size)` (max 100), so true backlog ≥101 is under-reported | Misleading backlog gauge |

---

## 2. Design Principles (decisions)

| # | Decision | Rationale |
|---|---|---|
| D1 | **Keep module-level metrics in `app/infrastructure/metrics.py`** — no DI registration | Current style; relay + tests already import them directly; no lifecycle needed (prometheus_client registries are global) |
| D2 | **One file grows into a metric registry with a documented table** (Section 4) | Prevents naming drift (G8) |
| D3 | **All new metrics carry `topic`/`chat`-type labels only where cardinality is bounded**; never label by `chat_oid`/user | Unbounded labels → Prometheus memory explosion |
| D4 | **Instrument at handler boundaries** (relay tick, consumer loop, WS manager, notification client), not inside the mediator | Handlers are the seams already covered by tests; mediator stays framework-free |
| D5 | **Metrics are non-fatal**: instrumentation must never break business flow (same convention as Telegram `try/except`) | Relay/consumer loops must survive telemetry failures |
| D6 | **Reuse `prometheus-client` + instrumentator** — no new libraries | Both already in `pyproject.toml` |

---

## 3. Metric Inventory (target state)

> Naming follows Prometheus conventions: `_total` suffix for counters, base name for gauges/histograms. `prometheus-client` auto-appends `_total` to Counters.

### 3.1 Already live (keep as-is)

| Metric | Type | Labels | Source |
|---|---|---|---|
| `http_requests_total` | Counter | handler, method, status | instrumentator (default) |
| `http_request_duration_seconds` | Histogram | handler, method, status | instrumentator (default) |
| `outbox_published_total` | Counter | — | relay |
| `outbox_publish_errors_total` | Counter | — | relay |
| `outbox_pending` | Gauge | — | relay |
| `kafka_messages_sent_total` | Counter | — | relay |

### 3.2 New metrics to add

| Metric | Type | Labels | Emitted by | Closes gap |
|---|---|---|---|---|
| `ws_connections_active` | Gauge | — | `ConnectionManager` | G1 |
| `ws_connections_accepted_total` | Counter | — | `ConnectionManager.accept_connection` | G1 |
| `ws_connections_removed_total` | Counter | — | `ConnectionManager.remove_connection` | G1 |
| `ws_messages_broadcast_total` | Counter | — | `ConnectionManager.send_all` | G1 |
| `ws_broadcast_failures_total` | Counter | — | `ConnectionManager.send_all` (per-socket exception) | G1 |
| `ws_broadcast_duration_seconds` | Histogram | — | `ConnectionManager.send_all` | G1 |
| `kafka_messages_consumed_total` | Counter | `topic` | `_kafka_consumer_loop` | G2, G9 |
| `kafka_consumer_errors_total` | Counter | `topic` | `_kafka_consumer_loop` exception path | G2 |
| `kafka_consumer_events_published_total` | Counter | `topic` | `_kafka_consumer_loop` after `mediator.publish` | G2 |
| `kafka_messages_sent_total` | **(modify)** | `topic` | relay | G9 |
| `outbox_publish_duration_seconds` | Histogram | `topic` | relay per-row send | G10-adjacent |
| `telegram_notifications_sent_total` | Counter | — | `ListenerAddedEventHandler` | G3 |
| `telegram_notifications_failed_total` | Counter | — | `ListenerAddedEventHandler` except-path | G3 |
| `db_operation_errors_total` | Counter | `operation` (`insert`/`update`/`delete`/`query`), `collection` | repo wrapper or handler try/except | G4 |
| `mediator_events_published_total` | Counter | `event` (class name) | `Mediator.publish` | — (flow volume) |
| `mediator_commands_handled_total` | Counter | `command` (class name) | `Mediator.handle_command` | — (flow volume) |
| `mediator_queries_handled_total` | Counter | `query` (class name) | `Mediator.handle_query` | — (flow volume) |
| `application_info` | Gauge | `version` | `create_app()` | G5 |

> **Cardinality guard (D3):** `event`/`command`/`query` labels use the **class name** of the
> mediator message — the set is closed (5 events, 4 commands, 4 queries today) and small.
> `topic` labels use the configured topic names (4 today). `chat_oid` is **never** a label.

---

## 4. Implementation Plan — Small Chunks

Each chunk is independently implementable and verifiable. Order matters only inside a
phase; phases are mostly sequential. Estimated sizes assume the house conventions
(single quotes, tabs, flat imports, `dataclass` style).

---

### Phase 1 — Foundations (no behavior change)

#### Chunk 1.1 ✅ — Restructure `app/infrastructure/metrics.py` as the metric registry
- **Files:** `app/infrastructure/metrics.py`
- **Actions:**
  1. Keep the four existing definitions **byte-identical** (relay + tests import them by name).
  2. Add a module docstring: "Single registry of Prometheus metrics. Import from here; never
     define ad-hoc metrics in feature modules."
  3. Add section comments (`# --- Outbox relay ---`, `# --- Kafka consumer ---`, etc.).
  4. Add the metric-to-owner table from Section 3 as a comment block (or reference this ADR).
- **Verify:** `cd app && poetry run pytest test/infrastructure/outbox/test_relay.py -v` — 3 relay tests still green (they assert absolute deltas, so no interference).
- **Risk:** Low. Import-only refactor.

#### Chunk 1.2 ✅ — Introduce a shared "safe observe" helper (optional but recommended)
- **Files:** `app/infrastructure/metrics.py`
- **Actions:**
  1. Add `def observe(fn, *args, **kwargs)` style helper (or per-metric `try/except` wrappers) that swallows exceptions from metric updates, logging at `WARNING`.
  2. Convention: business code calls e.g. `safe_inc(kafka_consumer_errors_total, 'topic', topic)`.
- **Verify:** unit test: helper never raises when the counter is mocked to raise. → `app/test/infrastructure/test_metrics.py` (9 tests).
- **Risk:** Low. Implements D5.
- **Implemented as:** `_safe_observe(description, update)` core + `safe_observe(fn, *args, **kwargs)` (any mutator), `safe_inc(counter, amount=1.0, **labels)` (labels via kwargs), `safe_set(gauge, value)`; failures log at WARNING via `logger.warning(..., exc_info=True)`. Label values are passed as kwargs (`safe_inc(counter, topic='chat-events')`) instead of the positional sketch above. Call sites in later chunks should use these helpers instead of raw `.inc()`/`.set()`.

#### Chunk 1.3 ✅ — Document the `/metrics` endpoint + metric list in `docs/`
- **Files:** `docs/architecture.md` (add "Observability" section), `README.md` (curl example `curl localhost:8000/metrics`), `docs/local-development.md` (Prometheus URL row).
- **Verify:** docs review only. → Implemented: "Observability" section added to `docs/architecture.md` (endpoint + full registry table + conventions), curl examples + registry pointer in `README.md`, metrics/Prometheus rows in the `docs/local-development.md` port map.
- **Risk:** None.

---

### Phase 2 — Relay & Kafka producer metrics (deepen what exists)

#### Chunk 2.1 ✅ — Fix the `outbox_pending` batch-size cap (G10)
- **Files:** `app/infrastructure/outbox/base.py`, `app/infrastructure/outbox/mongo.py`, `app/infrastructure/outbox/memory.py`, `app/infrastructure/outbox/relay.py`
- **Actions:**
  1. Add abstract `async def count_unsent(self) -> int` to `BaseOutboxRepository`.
  2. Mongo: `return await self.collection.count_documents({'sent': False})`.
  3. Memory: `return sum(1 for row in self._outbox if not row.sent)`.
  4. Relay `_tick()`: replace both `outbox_pending.set(len(rows))`/re-set-after-send with a single `outbox_pending.set(await self.outbox_repository.count_unsent())` at tick start; keep the post-send recompute.
  5. Update the two `AsyncMock`-based relay tests in `test_lifespan_outbox_broker.py` to set `repo.count_unsent = AsyncMock(return_value=0)` (they currently only mock `get_unsent`).
- **Verify:** `pytest test/infrastructure/outbox/ test/infrastructure/test_lifespan_outbox_broker.py -v`.
- **Risk:** Medium — touches an ABC; all implementations (mongo, memory, and any fakes in tests) must add the method. `FakeBroker`/`AsyncMock` repos in tests need updates.
- **Implemented as:** `count_unsent()` added to the ABC with a docstring explaining the G10 rationale; Mongo delegates to `count_documents({'sent': False})` (partial-index note inline); Memory sums unsent rows. Relay `_tick()` now sets `outbox_pending` once at tick start via `safe_set(outbox_pending, await count_unsent())` and re-sets it after `mark_as_sent` — both call sites use the Chunk 1.2 safe helpers (D5), as do `kafka_messages_sent_total` / `outbox_publish_errors_total` / `outbox_published_total`. Tests updated: the AsyncMock relay repos set `count_unsent`, plus three new tests — `test_mongo_outbox_count_unsent` (asserts the `{'sent': False}` filter), and `test_relay_outbox_pending_reflects_true_backlog_beyond_batch_size` (15 rows, `batch_size=10`: gauge reads 5 after the first tick, 0 after the second). Full suite: 148 passed.

#### Chunk 2.2 ✅ — Add `topic` label to `kafka_messages_sent_total` (G9)
- **Files:** `app/infrastructure/metrics.py`, `app/infrastructure/outbox/relay.py`
- **Actions:**
  1. Redefine `kafka_messages_sent_total = Counter('kafka_messages_sent_total', ..., ['topic'])`.
  2. Relay: `kafka_messages_sent_total.labels(topic=row.topic).inc()`.
  3. **Update tests:** label-less `get_sample_value('kafka_messages_sent_total')` returns `None` once labels exist; switch assertions to `get_sample_value('kafka_messages_sent_total', {'topic': ...})` or sum over children.
- **Verify:** relay tests green.
- **Risk:** Medium — breaking change for dashboards/tests reading the unlabelled series. Do **before** Grafana dashboards are built (Chunk 6.1).
- **Note:** `outbox_*` metrics intentionally stay label-free: per-row error attribution is not actionable.
- **Implemented as:** counter redefined with `['topic']` and docstring updated; relay calls `safe_inc(kafka_messages_sent_total, topic=row.topic)` (labels via kwargs per Chunk 1.2). `test_relay.py` switched to the labelled baseline-delta pattern: `METRIC_NAMES` no longer includes the metric, a `KAFKA_SENT_TOPICS` tuple + `_kafka_sent_value(topic)` helper provide per-topic baselines, and a new `test_relay_counts_sends_per_topic` asserts per-topic attribution. Dashboards/tests reading the unlabelled series updated in the same PR: Grafana "Kafka messages sent total" panel now queries `sum(rate(kafka_messages_sent_total[1m])) by (topic)` (JSON validated), plus `docs/architecture.md` registry row and `README.md` bullet. Full suite: 149 passed.

#### Chunk 2.3 ✅ — Relay timing histogram (optional)
- **Files:** `app/infrastructure/metrics.py`, `app/infrastructure/outbox/relay.py`
- **Actions:** add `outbox_publish_duration_seconds = Histogram(..., ['topic'])`; wrap the per-row `send_message` await with `time.perf_counter()`.
- **Verify:** new unit test asserting `_sum > 0` after a tick with one row (bounded label set).
- **Risk:** Low.
- **Implemented as:** `Histogram('outbox_publish_duration_seconds', ..., ['topic'])` added to the registry with an ownership-table row; relay times each per-row `send_message` attempt with `perf_counter()` and observes via `safe_observe(...labels(topic=...).observe, elapsed)` in a `finally` block — so **failed attempts are observed too** (timeout latency is exactly what you need during a Kafka outage). `started_at` is set *before* the `try` so the observation itself can never raise `NameError` (D5). Tests: `test_relay_observes_publish_duration_per_topic` (one observation per topic, `_sum > 0`) and `test_relay_observes_duration_on_failed_send_too` (delta == 1 and `_sum > 0` on the failure path). Grafana panel added: "Outbox publish latency p95 by topic" via `histogram_quantile(0.95, sum(rate(outbox_publish_duration_seconds_bucket[5m])) by (topic, le))`; registry row added to `docs/architecture.md` and README bullet. Full suite: 151 passed.

---

### Phase 3 — Inbound Kafka consumer metrics (G2)

#### Chunk 3.1 ✅ — Instrument `_kafka_consumer_loop`
- **Files:** `app/application/api/lifespan.py`, `app/infrastructure/metrics.py`
- **Actions:**
  1. Add counters `kafka_messages_consumed_total`, `kafka_consumer_errors_total`, `kafka_consumer_events_published_total` (all `topic`-labelled).
  2. In the loop: increment `consumed` per message (before parsing); increment `published` after successful `mediator.publish([event])`; increment `errors` in the `except` (pass `topic` via closure).
  3. Skip counting malformed payloads (missing `chat_oid`/`message`) as consumed-but-not-published; optionally count them under `kafka_consumer_errors_total` with a distinct reason — prefer a dedicated `kafka_consumer_malformed_total` counter (no labels) to keep semantics clean.
- **Verify:** unit test with a fake broker yielding 2 valid + 1 malformed message: consumed == 3, published == 2, malformed == 1.
- **Risk:** Low — loop already has `try/except`; this only adds observations.
- **Implemented as:** four new counters in the registry under a "Kafka consumer (inbound)" section (all via the ownership table); the loop increments `consumed` before parsing, `published` after `mediator.publish` returns, `malformed` (label-free) on the missing-`chat_oid`/`message` branch, and `errors` (topic-labelled) in the `except` — all through `safe_inc`. Note the loop consumes a **single** configured topic (`config.new_message_received_topic`), so the `topic` label is constant per process today but stays forward-compatible with multi-topic consumption. Tests: new `test/application/api/test_kafka_consumer_loop.py` with a `_BatchBroker` fake (yields a fixed batch, then blocks forever like an idle real consumer) — `test_consumer_loop_counts_consumed_published_malformed` asserts consumed==3/published==2/malformed==1, and `test_consumer_loop_counts_errors_without_dying` proves a mediator failure increments `errors` while the loop survives for the next message. Grafana: "Kafka consumed vs published by topic" + "Kafka consumer errors & malformed (rate)" panels; registry rows added to `docs/architecture.md` + README. Full suite: 153 passed.

#### Chunk 3.2 ✅ — Consumer liveness heartbeat (optional)
- **Files:** `app/application/api/lifespan.py`
- **Actions:** gauge `kafka_consumer_up` set to 1 in `start_kafka_consumer`, 0 in `stop_kafka_consumer`. Alert candidate later.
- **Verify:** manual or lifespan test asserting gauge transitions.
- **Risk:** Low.
- **Implemented as:** label-free `kafka_consumer_up` Gauge in the registry (ownership: consumer lifecycle). `start_kafka_consumer` sets 1 before creating the loop task and attaches a done-callback (`_drop_consumer_heartbeat_if_dead`) that clears the gauge only when the task terminated **unexpectedly** (`not cancelled() and exception() is not None`) — graceful shutdown must not double-fire the crash path; `stop_kafka_consumer` sets 0 first, so `task=None` no-ops still report down. Tests in `test_kafka_consumer_loop.py`: `test_consumer_heartbeat_transitions_on_start_and_stop` (+1 after start, net 0 after stop) and `test_consumer_heartbeat_drops_on_crash` (`_CrashingBroker` whose consumer iterator raises → gauge back to 0 via the done-callback). Alert candidate: `kafka_consumer_up == 0` while the app is up — feeds Chunk 7.2. Grafana: "Kafka consumer up (liveness heartbeat)" stat panel (also fixed a 3.1 grid collision between panels 9/10; layout validated collision-free). Full suite: 155 passed.

---

### Phase 4 — WebSocket metrics (G1)

#### Chunk 4.1 ✅ — Instrument `ConnectionManager`
- **Files:** `app/infrastructure/websockets/managers.py`, `app/infrastructure/metrics.py`
- **Actions:**
  1. `accept_connection`: `ws_connections_accepted_total.inc()`; recompute `ws_connections_active.set(len(...))` per key after append (sum across `connections_map`).
  2. `remove_connection`: `ws_connections_removed_total.inc()` only when a socket was actually removed; recompute active gauge.
  3. `send_all`: wrap per-socket `send_bytes` in `try/except` → `ws_broadcast_failures_total.inc()` per failure; `ws_messages_broadcast_total.inc()` once per successful fan-out call; observe `ws_broadcast_duration_seconds`.
  4. Keep `disconnect_all` behavior unchanged (it already sends + closes; a failure counter is out of scope here).
- **Verify:** extend `test/infrastructure/websockets/test_managers.py`: accept/remove transitions move the gauge; a socket whose `send_bytes` raises increments `ws_broadcast_failures_total` without breaking the loop over remaining sockets.
- **Risk:** Medium — `send_all` currently has **no** error handling; adding try/except changes behavior (one dead socket no longer aborts the fan-out). This is a bug fix, but call it out in the PR.

> **Note:** `disconnect_all` currently aborts the whole loop on the first dead socket — same latent bug. Fixing it is optional; if done, add `ws_broadcast_failures_total` there too and mention it in the PR.
- **Implemented as:** six metrics added to the registry under a "WebSocket manager" section (all label-free — keys are chat oids, unbounded per D3). `accept_connection` counts `ws_connections_accepted_total` and recomputes `ws_connections_active` inside the key lock; `remove_connection` counts `ws_connections_removed_total` **only when a socket was actually removed**; the active gauge is always recomputed from `connections_map` (sum over keys) rather than incremented/decremented, so it cannot drift. `send_all` wraps each per-socket `send_bytes` in `try/except` → `ws_broadcast_failures_total` per failure, increments `ws_messages_broadcast_total` once per completed fan-out call (including zero-socket calls), and observes `ws_broadcast_duration_seconds` in a `finally` — the **behavior fix** (one dead socket no longer aborts the fan-out) is covered by `test_dead_socket_counts_failure_without_breaking_fan_out`. `disconnect_all` is unchanged (per the chunk's note). All updates go through the Chunk 1.2 safe helpers (D5). Tests: `test_managers.py` extended with the baseline-delta pattern — accept/remove transitions move counters + gauge, gauge sums across keys, unknown-socket removal counts nothing, success/dead-socket/unknown-key fan-outs count and time correctly. Grafana: four WS panels (active, connects/disconnects rate, broadcast rate & failures, latency p95; layout validated collision-free). Registry rows added to `docs/architecture.md` + README. Full suite: 161 passed.

---

### Phase 5 — Mediator & DB metrics (G4 + flow visibility)

#### Chunk 5.1 ✅ — Mediator flow counters
- **Files:** `app/logic/mediator/base.py`, `app/infrastructure/metrics.py`
- **Actions:**
  1. `publish`: `mediator_events_published_total.labels(event=e.__class__.__name__).inc()` per event.
  2. `handle_command`: `mediator_commands_handled_total.labels(command=command_type.__name__).inc()` after handler list resolution (so `CommandHandlersNotRegisteredException` is **not** counted as handled).
  3. `handle_query`: `mediator_queries_handled_total.labels(query=query.__class__.__name__).inc()`.
- **Verify:** new `test/logic/test_mediator_metrics.py`: publish two events → counter == 2 with correct labels; unregistered command raises and does **not** increment.
- **Risk:** Medium — mediator is domain-adjacent; keep imports of `infrastructure.metrics` **inside the methods** or accept the architecture exception explicitly (see Open Question Q2). The project already imports infrastructure in `logic/events/messages.py`, so a top-level import is consistent with current practice.
- **Implemented as:** three `event`/`command`/`query`-labelled counters added to the registry under a "Mediator (CQRS flow volume)" section (class-name labels — closed, small set per D3); Q2 resolved as planned: top-level `from infrastructure.metrics import ...` in `mediator/base.py`. `publish` increments per event before dispatch; `handle_command` increments **after** handler resolution, so `CommandHandlersNotRegisteredException` is never counted as handled; `handle_query` increments before the handler call (flow volume — a domain exception like `ChatNotFoundException` is still counted as dispatched, verified in tests). All increments via `safe_inc` (D5). Tests: new `test/logic/test_mediator_metrics.py` (4 tests, baseline-delta pattern against `init_dummy_container`'s mediator) — per-class deltas on publish, command counted on success, unregistered command raises + does not count, query counted despite handler exception. Grafana: "Mediator events published by class" + "Mediator commands & queries handled" panels (18 panels total, layout validated collision-free); registry rows added to `docs/architecture.md` + README. Full suite: 165 passed.

#### Chunk 5.2 ✅ — DB failure counters
- **Files:** `app/logic/commands/messages.py` + `app/logic/queries/messages.py` (handler `try/except` blocks) or a thin wrapper; `app/infrastructure/metrics.py`
- **Actions:**
  1. Add `db_operation_errors_total = Counter(..., ['operation', 'collection'])`.
  2. Preferred injection point: the command/query handlers' existing exception paths (they already catch `ApplicationException`). Increment before re-raising.
  3. Do **not** instrument `MongoOutboxRepository` internals (the relay's own error counter already covers it).
- **Verify:** unit test: memory repo raising → counter increments, exception still propagates.
- **Risk:** Medium — touches every handler; keep it mechanical (one line per except).
- **Implemented as:** `db_operation_errors_total` Counter with `['operation', 'collection', 'exception']` labels; the exception class name is folded into `operation` (`insert.RuntimeError`) so the closed-vocabulary rule (D3) holds while failure classes stay queryable. The ADR's premise ("handlers already catch `ApplicationException`") did not match the code — handlers have no try/except — so the **preferred injection point became a thin wrapper** (the chunk's alternative): `_count_db_errors(operation, collection, call)` in `logic/commands/messages.py`, awaited per persistence call, counting via `safe_inc` (D5) and re-raising. Write paths wrap both the repository write and the outbox append (labeled `insert/{chats,messages,outbox}`; `delete` for chat deletion; `update` for telegram listeners) — a failed outbox write counts before `mediator.publish`, so no events leak out. Query handlers (`logic/queries/messages.py`) get the same wrapper around `get_chat_by_oid` / `get_all_chats` / `get_all_chat_listeners` / `get_messages` (`query/{chats,messages}`). Domain errors (`ChatNotFoundException`, duplicate title) are raised by the handlers *outside* the wrapped calls and stay uncounted. `MongoOutboxRepository` internals untouched per the chunk's rule. Tests: new `test/logic/test_db_failure_metrics.py` (5 tests, baseline-delta) — insert/delete/query failures counted + re-raised, outbox failure counted with no events published, domain exception not counted as DB failure. Grafana: "DB operation errors by operation & collection" panel (19 panels, layout validated collision-free); registry rows added to `docs/architecture.md` + README. Full suite: 170 passed.

---

### Phase 6 — Telegram notifications + app info (G3, G5)

#### Chunk 6.1 ✅ — Telegram notification counters
- **Files:** `app/logic/events/messages.py`, `app/infrastructure/metrics.py`
- **Actions:**
  1. Add `telegram_notifications_sent_total`, `telegram_notifications_failed_total`.
  2. In `ListenerAddedEventHandler.handle`: increment `sent` after successful `notification_client.send(...)`; increment `failed` in the existing `except` (alongside the current `logger.warning`).
  3. When `notification_client is None` (Telegram unconfigured), do **not** count anything — the metric measures delivery attempts only.
- **Verify:** extend `test/application/api/test_telegram_integration.py` (3 existing cases): success → `sent` +1; send raises → `failed` +1 and pipeline still completes; unconfigured → no change.
- **Risk:** Low.
- **Implemented as:** two label-free counters added to the registry under a "Telegram notifications (G3)" section — the metric measures delivery attempts only, so `notification_client is None` (unconfigured) counts nothing and unconfigured-silence stays distinguishable from failure. `ListenerAddedEventHandler.handle` increments `telegram_notifications_sent_total` after a successful `notification_client.send(...)` and `telegram_notifications_failed_total` in the existing `except` (alongside the unchanged `logger.warning`, D5). Tests: the 5 existing cases in `test_telegram_integration.py` extended with baseline-delta counter assertions — success +1 `sent`, failure +1 `failed` / +0 `sent` without raising, unconfigured +0/+0, and the end-to-end command→event→notification flow counts +1. Grafana: "Telegram notifications sent & failed" rate panel (20 panels, layout validated collision-free); registry rows added to `docs/architecture.md` + README. Full suite: 170 passed.

#### Chunk 6.2 ✅ — `application_info` gauge
- **Files:** `app/application/api/main.py`, `app/infrastructure/metrics.py`
- **Actions:**
  1. Add `application_info = Gauge('application_info', 'Build info', ['version'])`.
  2. In `create_app()`: `application_info.labels(version=app_version).info()` is tempting but wrong — set `.labels(version=...).set(1)` with `app_version = '0.1.0'` (or read from `pyproject`/env `APP_VERSION`).
- **Verify:** `curl :8000/metrics | grep application_info`.
- **Risk:** Low.
- **Implemented as:** `application_info` Gauge with a `version` label added to the registry under an "Application info (G5)" section — set to the constant 1 per the standard Prometheus build-info pattern (the ADR's warning about `.info()` was heeded; plain `.labels(...).set(1)` is used, via `safe_set` per D5). Version comes from a new `Config.app_version` setting (`APP_VERSION` env, default `'0.1.0'`) rather than a hardcoded constant, so deployments can report their tag without code changes; `create_app()` reads `Config()` directly (the settings class is env-backed and side-effect free). Test: `test_metrics_endpoint_exposes_application_info` in `test_messages_api.py` asserts both the REGISTRY sample (`application_info{version}=1.0`) and the raw `/metrics` body line. Grafana: "Deployed version (application_info)" stat panel (21 panels, layout validated collision-free); registry rows added to `docs/architecture.md` + README. Full suite: 171 passed.

---

### Phase 7 — Dashboards, alerts, docs (G6, G7, G8)

#### Chunk 7.1 ✅ — Grafana provisioning (G6)
- **Files (new):** `docker_compose/grafana/provisioning/datasources/prometheus.yml`, `docker_compose/grafana/provisioning/dashboards/dashboard-provider.yml`, `docker_compose/grafana/dashboards/chat-overview.json`
- **Actions:**
  1. Auto-provision the Prometheus datasource (`http://prometheus:9090`).
  2. One dashboard "Chat Overview" with rows: HTTP RED (rate/error/duration from instrumentator metrics), Outbox (published/errors/pending), Kafka (consumed/sent per topic), WebSocket (active/broadcasts/failures).
  3. `observability.yaml` already mounts `./grafana/provisioning` — no compose change needed.
- **Verify:** `make observability` → dashboard renders after generating traffic.
- **Risk:** Low.
- **Implemented as:** the provisioning files were built incrementally alongside the per-chunk panel work and follow the plan's layout with house naming — `provisioning/datasources/datasources.yaml` (auto-provisions **Prometheus** at `http://prometheus:9090`, default; plus **Loki** at `http://loki:3100` for the logs panel), `provisioning/dashboards/dashboards.yaml` (file provider scanning `/etc/grafana/provisioning/dashboards`), and `provisioning/dashboards/kafka-chat-overview.json` (22 panels: HTTP RED — request rate, latency p95, **5xx error rate** —, Outbox pending/published/errors/latency, Kafka sent/consumed-vs-published/errors/consumer-up per topic, WebSocket active/connects/broadcasts/latency, Mediator flow, DB errors, Telegram, `application_info`, and the live `{container="main-app"}` Loki logs panel). Datasource references use `${DS_PROMETHEUS}`/`${DS_LOKI}` templating variables; `observability.yaml` already mounts `./grafana/provisioning` — no compose change needed. Wrap-up validation: fixed panel id 10 (lost its `title`/`options` in the 3.1 edit), added the missing HTTP 5xx RED panel (id 22), and validated the dashboard programmatically — JSON parses, 22 unique panel ids, zero grid collisions, all 25 registry metrics referenced by queries.

#### Chunk 7.2 ✅ — Alert rules (G7) — *optional follow-up*
- **Files (new):** `docker_compose/prometheus-alerts.yml`, mount in `prometheus.yaml` via `rule_files`.
- **Candidate rules:**
  - `outbox_pending > 200 for 5m` (backlog growing; 200 calibrated from the
    2026-09-17 Locust baseline — originally the arbitrary Q1 placeholder 500)
  - `rate(outbox_publish_errors_total[5m]) > 0` (relay failing)
  - `kafka_consumer_up == 0` (consumer dead)
  - `rate(ws_broadcast_failures_total[5m]) > 0`
- **Verify:** `promtool check rules`.
- **Risk:** Low. Defer Alertmanager wiring.
- **Implemented as:** `docker_compose/prometheus-alerts.yml` with one `kafka-chat` group holding all four candidate rules — `OutboxBacklogGrowing` (`outbox_pending > 200 for 5m`, warning; the threshold was recalibrated from 500 to 200 on 2026-09-17 from a real Locust baseline — see Q1), `OutboxRelayFailing` (`rate(outbox_publish_errors_total[5m]) > 0`, warning), `KafkaConsumerDown` (`kafka_consumer_up == 0 and up{job='kafka-chat-api'} == 1`, critical — guarded by the app's `up` so a dead target doesn't double-page), and `WSBroadcastFailures` (`rate(ws_broadcast_failures_total[5m]) > 0`, warning) — each with `summary`/`description` annotations. Wired via `rule_files: [/etc/prometheus/prometheus-alerts.yml]` in `prometheus.yml` and a read-only volume mount in `docker_compose/prometheus.yaml` (rules evaluated every 15s with the global interval). Alertmanager wiring deferred per the chunk. Validation: structural YAML checks pass (group/rule shape, wiring into both config files) and the full suite is green; the literal `promtool check rules` run was still pending at implementation time (Docker daemon off) — closed by the follow-up notes below.
>
> **Follow-up validation (2026-09-17):** registry cross-check passed — every metric referenced by the four rules (`outbox_pending`, `outbox_publish_errors_total`, `kafka_consumer_up`, `ws_broadcast_failures_total`) exists in `app/infrastructure/metrics.py`; the `up{job='kafka-chat-api'}` matcher matches the scrape job in `prometheus.yml`; the `rule_files` entry matches the compose mount target; durations/severity/annotations structurally valid. Residual warnings (informational): three rules have no `for` clause — intentional for rate-window rules; `KafkaConsumerDown` is guarded by `up == 1` so a flapping single evaluation is acceptable. The literal `promtool check rules` run was still pending at that time — see below.
>
> **`promtool check rules` — EXECUTED 2026-09-17, residual closed.** Both checks pass:
> `docker run --rm --entrypoint=promtool -v ...:/check.yml prom/prometheus:latest check rules /check.yml`
> → `SUCCESS: 4 rules found`, and `promtool check config /etc/prometheus/prometheus.yml`
> (full config with `rule_files` wiring) → valid config, 1 rule file found, 4 rules.
> Note: on Prometheus v3 images the documented `docker run ... prom/prometheus:latest promtool ...`
> form fails (`unexpected promtool`) — the image entrypoint no longer dispatches to
> promtool; override with `--entrypoint=promtool` and pass `check rules <file>`.
>
> **`promtool check config` — re-executed 2026-09-25 after the alert additions (Docker up).**
> Full compose wiring validates: mounting the repo's `prometheus.yml` and
> `docker_compose/prometheus-alerts.yml` at their real container paths and running
> `promtool check config /etc/prometheus/prometheus.yml` → `SUCCESS: 1 rule files found`,
> `SUCCESS: 6 rules found`. Covers both alerts added since the 2026-09-17 run —
> `KafkaConsumerReconnecting` (O-1) and `OutboxRelayCircuitOpen`
> (`increase(circuit_breaker_rejected_total{name='kafka'}[15m]) > 0`, O-2) — including the new
> metric-reference expr. Same `--entrypoint=promtool` override as noted above.

#### Chunk 7.3 ✅ — Document the metric registry (G8)
- **Files:** `docs/adr/0006-metrics-implementation-plan.md` (this file — mark chunks ✅ as they land), `README.md`, `docs/architecture.md`.
- **Actions:** final table of all metrics with meaning, labels, alert-worthiness. Keep it in sync with `app/infrastructure/metrics.py`.
- **Risk:** None.
- **Implemented as:** `docs/architecture.md` (→ "Observability") carries the final registry table — all 25 metrics (23 from `app/infrastructure/metrics.py` + the 2 instrumentator defaults) with type, labels, owner, a one-line **meaning**, and an **alert-worthiness** verdict (✅ alerted / candidate / no, with the candidate rules named). A new "Alert rules (G7)" section documents the Chunk 7.2 rules (expression, severity, firing condition), the Q1 threshold placeholder, and the deferred Alertmanager wiring. `README.md` points at the full table and gained an "Alerts (Prometheus rules)" subsection. Verified in sync programmatically: every metric parsed from `metrics.py` (name, type, labels) matches the docs table one-to-one; the Chunk 7.2 alert expressions reference only registry metrics. All ADR chunks are now ✅.

---

## 5. Test Strategy

| Layer | Pattern (house style) | Files |
|---|---|---|
| Unit — relay | Fake broker + `MemoryOutboxRepository`; assert `REGISTRY.get_sample_value` deltas vs baselines | `test/infrastructure/outbox/test_relay.py` (exists, extend) |
| Unit — WS | Fake `WebSocket` objects; raise from `send_bytes` on one socket | `test/infrastructure/websockets/test_managers.py` (extend) |
| Unit — mediator | In-memory container fixture; assert labeled counters | `test/logic/test_mediator_metrics.py` (new) |
| Unit — consumer loop | Fake broker yielding valid/malformed dicts; fake mediator | `test/application/api/test_lifespan_app.py` or new file (extend) |
| Unit — telegram | Spy `BaseNotificationClient` (existing pattern) | `test/application/api/test_telegram_integration.py` (extend) |
| Suite | `cd app && poetry run pytest` (137 passing today) | full run per phase |

**Metric-test hygiene (already established in `test_relay.py`):**
- Capture baselines before acting, assert on **deltas** — the module-level registry is global across tests.
- After labelled metrics, `get_sample_value(name)` returns `None`; use `get_sample_value(name, {'topic': ...})`.
- Never `REGISTRY.unregister()` in app code; in tests, prefer baseline deltas over unregistration.

---

## 6. Rollout Order & Dependencies

```
Phase 1 (registry + docs)          ── no deps, do first
Phase 2 (relay fixes)              ── depends on P1 (registry)
Phase 3 (consumer)                 ── depends on P1
Phase 4 (websockets)               ── depends on P1
Phase 5 (mediator/db)              ── depends on P1; 5.1 informs 7.1 dashboard panels
Phase 6 (telegram/app-info)        ── depends on P1
Phase 7 (dashboards/alerts)        ── LAST — needs 2.2's label decision frozen
```

Phases 2–6 are independent of each other once Phase 1 lands, so they can be parallelized
across PRs: one PR per phase, each green in CI.

---

## 7. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|---|---|---|
| Label cardinality explosion (`chat_oid`, UUIDs as labels) | High if unchecked | D3 — closed label sets only; code review checklist item |
| Adding `topic` label breaks existing tests/dashboards | Certain (tests) | Chunk 2.2 updates tests in same PR; do before dashboards |
| `send_all` error handling changes behavior | Medium | Call out as bug fix in PR; add regression test |
| Global metric registry shared across tests → flaky assertions | Medium | Baseline-delta pattern (already used) |
| Mediator importing infrastructure metrics | Low (arch debate) | Consistent with `logic/events/messages.py` precedent; see Q2 |
| `count_unsent()` on huge outbox is a full scan | Low | Mongo `count_documents({'sent': False})` uses `sent` partial index if added; outbox is drained continuously |
| Double-instrumentation (relay + mediator both count Kafka sends) | Low | Ownership table in Section 3 — `kafka_messages_sent_total` belongs to relay only |

---

## 8. Open Questions

- **Q1 — Alert thresholds:** **RESOLVED 2026-09-17** via load-test calibration.
  Baseline (Locust, 50 users / spawn 5, 3 min, ~16 msg/s sustained, 2814 messages):
  `outbox_pending` max=35, p95=21, avg=6.4; relay published 2864 rows with 0 errors
  and drained to 0 after the run. The old placeholder 500 was never observable
  (0 samples above it) — set to **200** (~6x observed max, 2x the relay's per-tick
  drain capacity of batch_size=100 per 1s poll). Stats CSVs:
  `loadtest/baseline_outbox_*.csv`.
- **Q2 — Mediator metrics import:** top-level `from infrastructure.metrics import ...` in `app/logic/mediator/base.py` violates the "keep domain/logic free of infrastructure" guidance literally, but matches existing `logic/events/messages.py` practice. Decision: proceed with top-level import (pragmatic), revisit if `domain/` ever needs metrics.
- **Q3 — RED vs USE panels:** default RED for HTTP (instrumentator default), USE for WS/outbox. Confirm dashboard layout with whoever owns Grafana.
- **Q4 — Should `/metrics` move off the API port** to a dedicated one (avoids exposing metrics publicly if the API is internet-facing)? `prometheus_port` currently configures the Prometheus *server* port, not the app's. Deferred — current deployment is dev-only.

---

## 9. Acceptance Criteria (Definition of Done)

- [✅] All new metrics from Section 3.2 defined in `app/infrastructure/metrics.py` with section comments.
- [✅] `outbox_pending` reflects true backlog (not capped at `batch_size`).
- [✅] `kafka_messages_sent_total` carries `topic` label; tests updated.
- [✅] Consumer loop counts consumed/published/errors/malformed per topic.
- [✅] WS manager tracks active connections, fan-out successes/failures.
- [✅] Mediator emits event/command/query counters with class-name labels.
- [✅] Telegram handler emits sent/failed counters (no counts when unconfigured).
- [✅] `application_info` present on `/metrics`.
- [✅] Every chunk's tests pass: `cd app && poetry run pytest` (baseline 137 passing).
- [✅] Grafana "Chat Overview" dashboard auto-provisioned and rendering.
- [✅] Alert rules validated with `promtool check rules` (if Chunk 7.2 done) — executed 2026-09-17: `SUCCESS: 4 rules found`; full `check config` (rule_files wiring) also valid (see Chunk 7.2).
- [✅] Metric registry documented in README/architecture docs; this ADR's chunks marked ✅.

> Checked off 2026-09-12: all chunks implemented (171 tests passing). The last residual —
> the literal `promtool check rules` execution — was closed 2026-09-17 (see Chunk 7.2).
> Nothing pending.

---

## 10. References

- `app/infrastructure/metrics.py` — current metric definitions
- `app/infrastructure/outbox/relay.py` — relay instrumentation sites
- `app/application/api/lifespan.py` — consumer loop instrumentation site
- `app/infrastructure/websockets/managers.py` — WS instrumentation site
- `app/logic/mediator/base.py` — mediator instrumentation site
- `app/logic/events/messages.py` — Telegram handler instrumentation site
- `app/test/infrastructure/outbox/test_relay.py` — house metric-test pattern
- `prometheus.yml`, `docker_compose/prometheus.yaml`, `docker_compose/observability.yaml` — scrape + stack
- `docs/stateless-inventing-yao.md` — original outbox + metrics plan (implemented)
- `docs/adr/0005-websocket-kafka-relay-implementation-plan.md` — style reference for plan ADRs

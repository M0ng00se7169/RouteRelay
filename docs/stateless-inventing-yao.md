# Plan: Outbox Pattern + Prometheus Metrics

## Context

Today, every write command persists to MongoDB and then **synchronously** publishes
domain events straight to Kafka inside the same request (see
`app/logic/commands/messages.py:69,91,117` → `mediator.publish()` → handlers in
`app/logic/events/messages.py` → `message_broker.send_message(...)`).

This is a dual-write (no atomic guarantee). Two problems follow:
1. **Lost events** — if Kafka is unavailable *after* the Mongo write commits, the
   event is dropped and the domain/event state diverges silently.
2. **Tight coupling** — HTTP latency is bound to Kafka; a slow broker slows every write.

The **Transaction Outbox** pattern fixes this: write the business data and an outbox
row in **one MongoDB transaction**, then a background **relay** worker reads unsent
outbox rows and publishes them to Kafka. Kafka outages now only delay delivery, never
lose events.

Prometheus metrics are **absent** (`prometheus-client` not even a dependency). We add a
`/metrics` endpoint (HTTP + custom outbox/Kafka counters) and an optional Prometheus
container so the system is observable out of the box.

> **Key trade-off (chosen default):** the recommended approach uses a **MongoDB
> transaction**, which requires the standalone Mongo in `docker_compose/storages.yaml`
> to be a **single-node replica set** (`rs.initiate`). If you want *zero infra change*,
> the fallback is an **embedded-document outbox** (store outbox rows inside the same
> aggregate doc written by the handler, relying on single-document atomicity). That
> avoids the replica-set change but is less textbook. This plan implements the
> transaction version; the embedded variant is noted as a swap-out at the end.

---

## Approach

### 1. Outbox storage (transactional)
- New collection `outbox` (config `mongodb_outbox_collection`, default `outbox`).
  Each doc: `{ _id, event_id (str), topic (str), key (bytes), payload (bytes),
  occurred_at, sent (bool, default false), sent_at (datetime|null) }`.
- Repository methods accept an optional `session` so the aggregate write and the
  outbox insert share one `ClientSession` transaction.

### 2. Decouple Kafka from the write path
- Replace the Kafka-sending event handlers (`NewChatCreatedEventHandler`,
  `NewMessageReceivedEventHandler`, `ChatDeletedEventHandler`, `ListenerAddedEventHandler`)
  with **outbox-writing** handlers. They no longer call `message_broker.send_message`;
  they persist the event into the outbox (topic resolved from a central mapping).
- `mediator.publish()` still triggers in-process side effects, but Kafka delivery is
  now entirely the relay's job.
- A central **event→topic** resolver maps each `BaseEvent` subclass to its Kafka topic
  (reusing `config` topics already defined in `settings/config.py`).

### 3. Background relay worker (publishes outbox → Kafka)
- `OutboxRelay` spawned via **aiojobs** (already a dependency) on app startup, stopped
  on shutdown. Polls unsent rows, sends each to Kafka, then marks `sent=true`.
- Idempotent: Kafka key = event key; marking sent after a successful send means a crash
  between send and mark could re-send once (at-least-once) — acceptable for this app.

### 4. Metrics
- `prometheus-fastapi-instrumentator` instruments HTTP (latency, counts, in-flight).
- Custom counters/gauges in the relay: `outbox_published_total`,
  `outbox_publish_errors_total`, `outbox_pending` (gauge), `kafka_messages_sent_total`.
- Expose at `GET /metrics`.
- Add a Prometheus service + `prometheus.yml` scrape config and a `make prometheus`
  target so it scrapes `/metrics` immediately.

---

## Files to change / create

### New files
- `app/infrastructure/outbox/base.py` — `BaseOutboxRepository` ABC:
  `save_events(events, session=None)`, `get_unsent(limit)`, `mark_as_sent(ids)`.
- `app/infrastructure/outbox/mongo.py` — `MongoOutboxRepository` (uses `outbox`
  collection; stores serialized payload via `convert_event_to_broker_message`).
- `app/infrastructure/outbox/memory.py` — `MemoryOutboxRepository` (for tests; mirrors
  `infrastructure/repositories/messages/memory.py`).
- `app/infrastructure/outbox/relay.py` — `OutboxRelay` (aiojobs loop + custom metrics).
- `app/infrastructure/metrics.py` — Prometheus metric definitions (module-level).
- `app/infrastructure/outbox/mapper.py` — `resolve_topic(event) -> str` (event class →
  `config` topic; mapping built in `logic/init.py`).
- `docker_compose/prometheus.yaml` + `docker_compose/prometheus.yml` (scrape config).
- `app/test/infrastructure/outbox/test_relay.py` — unit test: unsent rows get sent &
  marked; error path increments error counter.

### Modified files
- `app/settings/config.py` — add `mongodb_outbox_collection` (default `outbox`),
  `outbox_relay_poll_interval` (default `1.0`), and `prometheus_port` (default `9090`).
- `app/infrastructure/repositories/messages/base.py` — add optional `session` param to
  `add_chat`, `add_message`, `add_telegram_listener`, `delete_chat_by_oid`.
- `app/infrastructure/repositories/messages/mongo.py` — thread `session` into
  `insert_one`/`update_one`/`delete_one`; register one transaction.
- `app/infrastructure/repositories/messages/memory.py` — accept & ignore `session`.
- `app/logic/events/messages.py` — rewrite the 4 Kafka handlers as **outbox handlers**
  (write to `BaseOutboxRepository` instead of `send_message`). Keep
  `NewMessageReceivedFromBrokerEventHandler` untouched (consumer-side).
- `app/logic/commands/messages.py` — open a `ClientSession`, run aggregate write **and**
  `outbox_repository.save_events(...)` inside `async with session.start_transaction():`,
  then commit. Remove the direct `mediator.publish()` Kafka dependency (keep publishing
  for in-process effects if any). Apply to `CreateChat`, `CreateMessage`, `DeleteChat`,
  `AddTelegramListener` handlers.
- `app/logic/init.py` — register `BaseOutboxRepository` (mongo + memory variants),
  build the event→topic mapping, swap Kafka handlers for outbox handlers, resolve
  `OutboxRelay`, register `OutboxRelay` for lifespan startup.
- `app/application/api/lifespan.py` — start broker **and** `OutboxRelay` (aiojobs
  scheduler) on startup; stop relay + broker on shutdown.
- `app/application/api/main.py` — instrument app with `Instrumentator()` and expose
  `/metrics` inside `create_app()`.
- `docker_compose/storages.yaml` — convert `mongodb` to a single-node replica set
  (`command: ["--replSet","rs0"]`), add an `init-mongo` service that runs
  `rs.initiate()`, update healthcheck. Update `.env` connection URI with
  `?replicaSet=rs0`.
- `pyproject.toml` — add `prometheus-fastapi-instrumentator` (pulls `prometheus-client`).
- `Makefile` — add `prometheus` / `observability` target composing the new service.
- `app/test/fixtures.py` & `app/test/application/api/conftest.py` — register
  `MemoryOutboxRepository` (singleton) so existing tests keep passing.
- `README.md` — document `/metrics`, the outbox + relay, replica-set requirement, and
  new Makefile/Prometheus targets.

### Reused existing utilities
- `app/infrastructure/message_brokers/converters.py:convert_event_to_broker_message` —
  serializes events to bytes for outbox payloads (no new serializer).
- `app/infrastructure/message_brokers/kafka.py:KafkaMessageBroker.send_message` — used
  by the relay to publish (signature already `send_message(key, topic, value)`).
- `app/domain/events/base.py` `BaseEvent` (`event_id`, `occurred_at`) — keys/ordering.
- `aiojobs` (already in `pyproject.toml`) — relay scheduler.

---

## Verification

1. **Unit / relay test** (`pytest`): `MemoryOutboxRepository` seeded with unsent rows;
   run `OutboxRelay` one tick against a fake broker; assert rows marked sent and
   `outbox_published_total` increments; inject a broker failure and assert
   `outbox_publish_errors_total` increments and rows remain unsent.
2. **API smoke** (existing suite stays green with memory repos):
   `poetry run pytest` — confirms command handlers + DI wiring still work.
3. **End-to-end with Docker**:
   - `make all` (now brings up replica-set Mongo, Kafka, app, Prometheus).
   - `POST /chat/` then `POST /chat/{oid}/messages` — confirm message lands in Mongo
     **and** Kafka topic via Kafka UI (`:8090`) even after first confirming the relay.
   - Kill Kafka mid-test: writes still succeed (outbox accumulates); restart Kafka →
     relay drains the outbox (Kafka UI shows the backlog delivered). This proves the
     dual-write hazard is gone.
   - `curl localhost:{API_PORT}/metrics` — see `http_requests_*` plus
     `outbox_published_total`, `outbox_pending`, `kafka_messages_sent_total`.
   - Prometheus UI (`:9090`) scrapes `/metrics` and graphs the above.
4. **Lint/format**: `poetry run ruff check .` and `poetry run pre-commit run --all-files`.

---

## Fallback variant (if replica-set change is unwanted)
Swap `MongoOutboxRepository` for an **embedded-document outbox**: command handlers push
the serialized event into a `chat.outbox` array within the *same* `insert_one`/`update_one`
call already performed (no `ClientSession`/transaction, no `docker_compose` change). The
relay then reads `outbox` arrays, publishes, and `$pull`s each entry. Everything else in
this plan (relay, metrics, `/metrics`, handlers) is identical.

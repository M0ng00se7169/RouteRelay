# Decomposition of `docs/stateless-inventing-yao.md` — Transaction Outbox + Prometheus metrics

## Context

Today, every write command persists to MongoDB and then **synchronously** publishes domain events
straight to Kafka inside the same HTTP request (`CreateChat`/`CreateMessage`/`DeleteChat`/
`AddTelegramListener` handlers → `self._mediator.publish()` → Kafka-sending event handlers →
`message_broker.send_message(...)`). This is a dual-write with no atomic guarantee: if Kafka is
unavailable *after* the Mongo write commits, the event is dropped and domain/Kafka state diverges
silently; HTTP latency is also bound to Kafka.

The **Transaction Outbox** pattern fixes this: write the business data and an outbox row in **one
MongoDB transaction**, then a background **relay** worker reads unsent outbox rows and publishes them
to Kafka. Kafka outages only delay delivery, never lose events. The plan also adds **Prometheus
metrics** (`/metrics` + custom outbox/Kafka counters) for observability.

### Reconciled findings vs. the plan doc (read during exploration — fold these in)
- **`convert_event_to_broker_message` is `orjson.dumps(event)`** (`converters.py:5`). `orjson` does
  **not** serialize arbitrary `@dataclass` instances without `OPT_SERIALIZE_DATACLASS`. Verify before
  reusing it for outbox payloads; add the option or a dict conversion if it raises.
- **No `ClientSession` exists anywhere.** `AsyncIOMotorClient` is created in `init.py:78-81` and
  `Mongo*Repository` exposes `_collection` (`mongo.py:33-35`). The relay needs the client (or a
  `create_session` helper) registered so handlers can `start_transaction`.
- **`punq` is a dev dependency** (`pyproject.toml:26`) but imported at runtime in `init.py`. Worth
  promoting to `[tool.poetry.dependencies]` (separate small task) — note as a caveat.
- **`aiojobs` is present but unused** (`pyproject.toml:19`) — usable for the relay scheduler, no
  install strictly required.
- **`ListenerAddedEventHandler.handle` is mis-annotated `NewChatCreatedEvent`** (`events/messages.py:31`,
  from `known-issues.md` #1) — its rewrite in Group D fixes this incidentally; key on
  `event.listener_oid` (the event has no `event_id`).
- **`NewMessageReceivedFromBrokerEventHandler` must stay untouched** (`events/messages.py:58-64`).
- **Mongo is a standalone** (`docker_compose/storages.yaml`) — transactions require converting it to a
  replica set (`--replSet rs0` + `init-mongo`).

---

## Group 0 — Pre-flight verification (do first, unblocks serialization)

### M0 — Verify/replace event→bytes serialization
- **Files:** `app/infrastructure/message_brokers/converters.py`, `app/domain/events/messages.py`
- **Action:** Unit-test that `convert_event_to_broker_message` round-trips each domain event
  (`NewChatCreatedEvent`, `NewMessageReceivedEvent`, `ChatDeletedEvent`, `ListenerAddedEvent`). If
  `orjson.dumps(dataclass)` raises, add `orjson.dumps(event, option=orjson.OPT_SERIALIZE_DATACLASS)`
  (or convert to dict first). This serializer is reused by the outbox writer in M3/M8, so it must be
  correct before anything else.
- **Verify:** new test `test_converter_roundtrip` passes.

## Group A — Config & settings

### M1 — Add outbox + metrics settings
- **Files:** `app/settings/config.py:9` (after `mongodb_messages_collection`); `.env:3`
- **Action:** Add to `Config`: `mongodb_outbox_collection: str = Field(default='outbox', alias='MONGODB_OUTBOX_COLLECTION')`,
  `outbox_relay_poll_interval: float = Field(default=1.0, alias='OUTBOX_RELAY_POLL_INTERVAL')`,
  `prometheus_port: int = Field(default=9090, alias='PROMETHEUS_PORT')`. Add matching `.env` entries.
- **Verify:** `poetry run ruff check .`; instantiate `Config()` and assert new fields resolve.

## Group B — Outbox storage layer

### M2 — `BaseOutboxRepository` ABC
- **Files:** `app/infrastructure/outbox/base.py` (NEW), `app/infrastructure/repositories/messages/base.py` (pattern reference)
- **Action:** `@dataclass` ABC mirroring the existing repo style: `async def save_events(self, events: list[BaseEvent], session=None)`,
  `async def get_unsent(self, limit: int) -> list[OutboxRow]`, `async def mark_as_sent(self, ids: list[str])`.
  Outbox row shape: `{_id, event_id, topic, key, payload, occurred_at, sent, sent_at}`.
- **Verify:** `ruff`.

### M3 — `MongoOutboxRepository`
- **Files:** `app/infrastructure/outbox/mongo.py` (NEW)
- **Action:** Use the `outbox` collection (from `Config.mongodb_outbox_collection`). `save_events`
  inserts serialized payload via the M0 converter, `topic` resolved through the mapper (M9), `key`
  from `str(event.event_id).encode()`. Thread `session` into `insert_many`. `get_unsent` queries
  `sent:false`, `mark_as_sent` sets `sent:true, sent_at:now`.
- **Verify:** `ruff`; unit test against a fake/real Mongo (mark behind Mongo marker).

### M4 — `MemoryOutboxRepository`
- **Files:** `app/infrastructure/outbox/memory.py` (NEW); mirror `repositories/messages/memory.py`
- **Action:** In-list store; `save_events`/get_unsent/mark_as_sent operate on `field(default_factory=list)`.
  Accept & ignore `session` (single-doc atomicity assumption). This is the test double.
- **Verify:** `ruff`.

### M5 — Register outbox repo in DI
- **Files:** `app/logic/init.py` (beside `:98-99`), `app/test/fixtures.py`
- **Action:** `container.register(BaseOutboxRepository, factory=init_mongodb_outbox_repository, scope=Scope.singleton)`
  in production; in `init_dummy_container` register `MemoryOutboxRepository`. Resolve `BaseOutboxRepository`
  into the relay (M12) and outbox handlers (M10).
- **Verify:** `poetry run pytest` (fixtures still build the container).

## Group C — Thread `session` through write repositories

### M6 — Add optional `session` to repo ABCs
- **Files:** `app/infrastructure/repositories/messages/base.py` (`BaseChatsRepository`, `BaseMessagesRepository`)
- **Action:** Add `session=None` to `add_chat`, `add_message`, `add_telegram_listener`, `delete_chat_by_oid`.
- **Verify:** `ruff`.

### M7 — Thread `session` in Mongo impl + expose client
- **Files:** `app/infrastructure/repositories/messages/mongo.py` (`:53,:68,:71,:82-84`), `app/logic/init.py:78-81`
- **Action:** Pass `session=session` into each `insert_one`/`update_one`/`delete_one`. Register the
  raw `AsyncIOMotorClient` (or a `create_session` helper) in `init.py` so command handlers can open a
  `ClientSession` and `start_transaction`. Memory variants accept & ignore `session`.
- **Verify:** `poetry run pytest` (memory path unaffected); Mongo write test behind marker.

## Group D — Decouple Kafka from the write path (outbox handlers)

### M8 — Rewrite the 4 Kafka handlers as outbox handlers
- **Files:** `app/logic/events/messages.py` (`NewChatCreatedEventHandler`, `ListenerAddedEventHandler`, `NewMessageReceivedEventHandler`, `ChatDeletedEventHandler`)
- **Action:** Replace `message_broker.send_message(...)` with `await self.outbox_repository.save_events([event])`.
  Fix the `ListenerAddedEventHandler` annotation (`:31` → `ListenerAddedEvent`) and key on `event.listener_oid`.
  Keep `ChatDeletedEventHandler.disconnect_all(...)` side effect (it is a WS concern, not Kafka).
  Leave `NewMessageReceivedFromBrokerEventHandler` untouched.
- **Verify:** `ruff`; unit test that publishing a domain event writes an outbox row (memory repo).

### M9 — Event→topic resolver
- **Files:** `app/infrastructure/outbox/mapper.py` (NEW); `app/settings/config.py` (topic fields already exist: `:11,:12,:13,:14,:15`)
- **Action:** `resolve_topic(event: BaseEvent) -> str` mapping each domain event class to its configured
  Kafka topic. Normalize the `event_title` vs `title` inconsistency (some events use `title`, some
  `event_title` — confirmed in `domain/events/messages.py`).
- **Verify:** unit test mapping each event class.

### M10 — Swap handlers in `init.py`
- **Files:** `app/logic/init.py` (`:140-168` build sites, `:170-217` registrations)
- **Action:** Build the 4 outbox handlers with `BaseOutboxRepository` + mapper; register them for their
  events in place of the Kafka handlers. Build the `event→topic` mapping once and pass to the mapper.
- **Verify:** `pytest`.

## Group E — Command handlers open a transaction

### M11 — Wrap business write + outbox save in a transaction
- **Files:** `app/logic/commands/messages.py` (`CreateChatCommandHandler:32-46`, `CreateMessageCommandHandler:55-71`, `DeleteChatCommandHandler:80-91`, `AddTelegramListenerCommandHandler:101-117`)
- **Action:** For each: open `ClientSession` via the registered client, `async with session.start_transaction():`,
  call the repo write **and** `outbox_repository.save_events(events, session=session)` inside the
  transaction, commit, then call `self._mediator.publish(entity.pull_events())` (after commit, for
  in-process side effects like WS disconnect). **Caveat/alt:** an arguably cleaner hook is to intercept
  inside `Mediator.publish` so the outbox write reuses the same session — but the plan doc explicitly
  chooses per-handler transactions, so follow that and flag the alternative in a code comment.
- **Verify:** `pytest` (memory repos ignore session → existing tests stay green); Mongo integration test behind marker asserting outbox row exists after a committed write.

## Group F — Relay worker

### M12 — `OutboxRelay` (aiojobs loop)
- **Files:** `app/infrastructure/outbox/relay.py` (NEW)
- **Action:** `async def run(self)`: loop `get_unsent(limit)`, for each row call
  `KafkaMessageBroker.send_message(topic=row.topic, key=row.key, value=row.payload)`, then
  `mark_as_sent([ids])`. Use `aiojobs` scheduler. Increment custom metrics (M14). Idempotent
  (at-least-once): Kafka key = event key; crash between send and mark may re-send once — acceptable.
- **Verify:** unit test `test_relay` (memory outbox + fake broker): unsent rows get sent & marked;
  inject broker failure → error counter increments, rows stay unsent.

## Group G — Metrics

### M13 — Instrument app + expose `/metrics`
- **Files:** `app/application/api/main.py:19-30`; `pyproject.toml` (add `prometheus-fastapi-instrumentator`)
- **Action:** Add dependency; in `create_app()` do `PrometheusInstrumentator().instrument(app).expose(app, endpoint='/metrics')`.
- **Verify:** `ruff`; app boots, `GET /metrics` returns text.

### M14 — Custom outbox/Kafka metrics
- **Files:** `app/infrastructure/metrics.py` (NEW); used by `relay.py` (M12)
- **Action:** Define module-level `Counter`/`Gauge`: `outbox_published_total`, `outbox_publish_errors_total`,
  `outbox_pending`, `kafka_messages_sent_total`. Increment in the relay loop.
- **Verify:** unit test asserts counters move after a relay tick.

## Group H — Lifespan wiring

### M15 — Start/stop broker + relay in lifespan
- **Files:** `app/application/api/lifespan.py` (`init_message_broker`/`close_message_broker`), `main.py:12-16`
- **Action:** After `init_message_broker()`, start the `aiojobs` scheduler and `OutboxRelay` task; on
  shutdown, stop the relay task, close the scheduler, then `close_message_broker()`. Resolve `OutboxRelay`
  from the same cached container. For API tests (`TestClient` runs lifespan, `conftest.py:20`), ensure the
  relay is overridden with a no-op/fake so tests don't require Kafka.
- **Verify:** app boots; `pytest` (relay overridden in API conftest).

## Group I — Infra: replica set + Prometheus container

### M16 — Convert Mongo to single-node replica set
- **Files:** `docker_compose/storages.yaml` (`mongodb` service:2-10)
- **Action:** Add `command: ["--replSet","rs0"]`, a `healthcheck` (e.g. `mongosh --eval "db.adminCommand('ping')"`), keep volume + `backend` network.
- **Verify:** `make storages` then `docker exec` `rs.status()` shows replica set.

### M17 — Add `init-mongo` one-shot service
- **Files:** `docker_compose/storages.yaml` (new service)
- **Action:** `mongo:6-jammy` one-shot that waits for `mongodb` healthy then runs `rs.initiate()`. `depends_on` the mongodb healthcheck.
- **Verify:** after `make storages`, replica set is initiated automatically.

### M18 — Update `.env` Mongo URI for replica set
- **Files:** `.env:3`
- **Action:** `MONGO_DB_CONNECTION_URI=mongodb://mongodb:27017?replicaSet=rs0` so `motor` discovers topology for transactions.
- **Verify:** app connects; a transaction succeeds.

### M19 — Add Prometheus service + scrape config
- **Files:** `docker_compose/prometheus.yaml` (NEW), `prometheus.yml` (scrape config, NEW)
- **Action:** `prometheus` service on `backend` network, ports `9090:9090`, bind-mount `prometheus.yml`
  scraping `main-app:<prometheus_port>/metrics`.
- **Verify:** `make prometheus` → UI at `:9090` shows targets up.

### M20 — Extend Makefile
- **Files:** `Makefile:5-7`
- **Action:** Add `PROMETHEUS_FILE = docker_compose/prometheus.yaml`; append `-f $(PROMETHEUS_FILE)` to
  `all`/`all-down`; add `prometheus`/`prometheus-down`/`prometheus-logs` targets mirroring existing pattern.
- **Verify:** `make all` brings up storages+app+kafka+prometheus.

## Group J — Tests & docs

### M21 — Relay unit test
- **Files:** `app/test/infrastructure/outbox/test_relay.py` (NEW)
- **Action:** Seed `MemoryOutboxRepository` with unsent rows; run `OutboxRelay` one tick against a fake
  broker; assert rows marked sent + `outbox_published_total` increments; inject broker failure →
  `outbox_publish_errors_total` increments, rows remain unsent.
- **Verify:** `pytest app/test/infrastructure/outbox/test_relay.py`.

### M22 — Test fixtures for outbox
- **Files:** `app/test/fixtures.py`, `app/test/conftest.py` (+ `application/api/conftest.py`)
- **Action:** Register `MemoryOutboxRepository` (singleton) so existing tests keep passing; add an
  `outbox_repository` fixture; override the relay in API conftest so lifespan doesn't require Kafka.
- **Verify:** `poetry run pytest` green.

### M23 — Docs
- **Files:** `README.md`, `CLAUDE.md` ("Known gotchas"), `docs/`
- **Action:** Document `/metrics`, the outbox + relay, the replica-set requirement (transactions), and
  the new Makefile/Prometheus targets.
- **Verify:** docs render; links valid.

---

## Verification (whole feature)
```bash
poetry run ruff check .
poetry run pre-commit run --all-files
poetry run pytest                       # unit + API tests with memory repos; Mongo/Kafka behind markers
# End-to-end with Docker:
make all                                # replica-set Mongo + Kafka + app + Prometheus
POST /chat/ then POST /chat/{oid}/messages   # message lands in Mongo AND Kafka UI even after relay
kill Kafka mid-test → writes still succeed (outbox accumulates); restart → relay drains backlog
curl localhost:<API_PORT>/metrics       # http_* + outbox_published_total + outbox_pending + kafka_messages_sent_total
# Prometheus UI :9090 scrapes /metrics
```

## Suggested execution order (dependency-aware)
1. **M0** (serialization pre-flight)
2. **M1** → **M2** → **M3** → **M4** → **M5** (outbox storage)
3. **M6** → **M7** (session threading)
4. **M9** → **M8** → **M10** (outbox handlers + mapper)
5. **M11** (transactions in command handlers)
6. **M14** → **M12** → **M13** → **M15** (relay + metrics + lifespan)
7. **M16** → **M17** → **M18** (replica set)  ← infra, risky, can be parallel-tracked
8. **M19** → **M20** (Prometheus container + Makefile)
9. **M21** → **M22** → **M23** (tests + docs)

## Fallback variant (if replica-set change is unwanted)
Swap `MongoOutboxRepository` for an **embedded-document outbox**: command handlers push the serialized
event into a `chat.outbox` array within the *same* `insert_one`/`update_one` already performed (no
`ClientSession`/transaction, no docker change). The relay then reads `outbox` arrays, publishes, and
`$pull`s each entry. Everything else (relay M12, metrics M14, `/metrics` M13, handlers M8, lifespan M15)
is identical. M16/M17/M18 become optional.

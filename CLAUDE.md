# CLAUDE.md

Guidance for working in this repository (Simple Kafka Chat — a DDD + CQRS + event-driven FastAPI reference app).

## What this project is

A multi-user chat backend built to demonstrate **Domain-Driven Design (DDD)**, **CQRS**, and **event-driven architecture** with **Apache Kafka**. Chats and messages live in **MongoDB**; clients subscribe to live updates over **WebSockets**; domain events are published to Kafka topics. See `README.md` for the full narrative, API reference, and getting-started steps.

## Architecture at a glance

Layered, DDD-style layout under `app/`:

```
app/
├── application/   # HTTP/WebSocket API (FastAPI routers, schemas, lifespan)
├── domain/        # Entities, value objects, domain events, exceptions
├── logic/         # Commands, queries, event handlers, Mediator, DI container
├── infrastructure/ # MongoDB repos, Kafka broker, WebSocket manager, adapters
├── settings/      # Environment-based configuration (pydantic-settings)
└── test/          # Unit and API tests (in-memory repositories for isolation)
```

Core patterns:

- **CQRS + Mediator** — API handlers delegate to a `Mediator` (`app/logic/mediator/base.py`) that routes **commands** (writes), **queries** (reads), and **domain events** (side effects).
- **Rich domain model** — `Chat`/`Message`/`ChatListener` entities (in `app/domain/entities/messages.py`) register domain events when state changes and expose `pull_events()` to drain them.
- **Event publishing** — Command handlers persist to Mongo, then call `self._mediator.publish(entity.pull_events())`. Event handlers (in `app/logic/events/messages.py`) are where side effects like Kafka sends or WebSocket pushes happen.
- **Dependency injection** — A single `punq` container in `app/logic/init.py` (`init_container()`, `@lru_cache(1)`) wires Config, the Mongo client, repositories, the message broker, the connection manager, the mediator, and all handlers. **This is the single source of truth for wiring** — add new handlers/repos here.
- **Import aliases** — First-party packages are importable flat: `application`, `domain`, `infrastructure` (alias `infra` is configured for isort, but code uses `infrastructure`), `logic`, `settings`, `test`. The app is run with `app/` on the path (`uvicorn --factory application.api.main:create_app`); tests are run from within `app/` (see `conftest.py`).

### Request flow (create message)

`POST /chat/{id}/messages` → `application/api/messages/handlers.py` → `mediator.handle_command(CreateMessageCommand)` → `CreateMessageCommandHandler` (validates chat exists, builds `Message`, saves via `messages_repository`, `publish(chat.pull_events())`) → registered event handlers (`NewMessageReceivedEventHandler` sends to Kafka) → response.

## Conventions

- **Handlers are frozen `@dataclass`es** with dependencies as typed fields. Commands are `@dataclass(frozen=True)` with a value payload. Follow the existing shape in `app/logic/commands/messages.py` / `app/logic/events/messages.py` when adding new ones.
- **Repositories** live under `app/infrastructure/repositories/messages/` in three flavors: `base.py` (ABCs), `mongo.py` (Motor, production), `memory.py` (in-memory, tests). Add new persistence behind the ABC so tests can use the memory variant.
- **Config** is a single `pydantic-settings` `Config` (`app/settings/config.py`) with env aliases (e.g. `MONGO_DB_CONNECTION_URI`). Add new settings there and read them in `init_container`.
- **Serialization** uses `orjson` (see `app/infrastructure/message_brokers/converters.py:convert_event_to_broker_message`) for events going to Kafka — prefer it over `json`.
- **Formatting/linting**: Ruff + isort + pre-commit. Ruff line-length is **100**, quote-style is **single**, indent-style is **tab**. isort recognizes `fastapi`/`starlette` sections and first-party `application/domain/infra/logic/settings/tests`. Run `poetry run pre-commit run --all-files` before committing.

## Common commands

```bash
# Local dev (needs reachable Mongo + Kafka at configured URLs)
poetry install
poetry run uvicorn --factory application.api.main:create_app --reload --host 0.0.0.0 --port 8000

# Tests
poetry run pytest

# Lint / format
poetry run ruff check .
poetry run pre-commit run --all-files
```

### Docker Compose (Makefile targets)

- `make all` — bring up storages + app + Kafka
- `make storages` / `make kafka` / `make app` — individual stacks (`docker_compose/*.yaml`)
- `make all-down` / `app-down` / `storages-down` / `kafka-down`
- `make app-shell` / `make app-logs`

Service URLs (after `make all`): API docs `http://localhost:8000/api/docs`, Mongo Express `:28081`, Kafka UI `:8090`, Prometheus `:9090`. Metrics at `GET /metrics`.

## Known gotchas / things to verify before assuming

- **MongoDB runs as a replica set**: `docker_compose/storages.yaml` starts Mongo with `--replSet rs0` plus a one-shot `init-mongo` service that runs `rs.initiate()`. Command handlers open a `ClientSession` transaction when persisting, so `.env` must use `MONGO_DB_CONNECTION_URI=mongodb://mongodb:27017?replicaSet=rs0`. **If you swap Mongo back to a standalone, the transaction-based writes will fail.**
- **Writes go through a Transaction Outbox, not Kafka directly**: `logic/events/messages.py` handlers now save domain events to `BaseOutboxRepository` (the outbox collection). A background **relay** (`infrastructure/outbox/relay.py`) polls the outbox and publishes to Kafka. So a Kafka outage delays but never drops events; write latency is decoupled from broker availability.
- **Broker→WebSocket consumer loop is not wired**: `KafkaMessageBroker` defines consume/relay behavior, but the consumer that pushes broker messages to WebSocket clients is not started in the app lifespan (`app/application/api/lifespan.py` only starts/stops the broker + relay). The `NewMessageReceivedFromBrokerEvent`/`...Handler` path exists but is currently dead unless something starts consuming.
- **WebSocket manager** is a singleton `ConnectionManager` resolved from the container and used by both event handlers (push to clients) and the WS router (`app/application/api/messages/websockets/messages.py`).
- **Tests use in-memory repos** via `app/test/fixtures.py` and `app/test/application/api/conftest.py`. New repositories must be registered there (mirroring `MongoDBChatsRepository`/`MongoDBMessagesRepository`) or tests break.
- **The relay is overridden in tests**: `init_dummy_container` registers a `_NoopOutboxRelay` (and swaps Mongo for memory). The lifespan helpers (`init_message_broker`/`close_message_broker`/`start_relay`) resolve the container through `app.dependency_overrides[init_container]`, so the conftest override also drives the broker + relay to test doubles — no Kafka needed to run `pytest`.

## Before modifying

1. Read `app/logic/init.py` to see how the piece you're touching is wired.
2. Respect the `base`/`mongo`/`memory` repository split — never import Motor directly in domain/logic layers.
3. Keep domain entities free of infrastructure imports; keep `logic/` depending only on `domain/` and `infrastructure/` interfaces.

## Agent skills

### Issue tracker

Issues and specs live as local markdown files under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical roles: needs-triage, needs-info, ready-for-agent, ready-for-human, wontfix. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` at the repo root plus `docs/adr/`. See `docs/agents/domain.md`.

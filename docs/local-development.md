# Local Development

Operational walkthrough for running this app locally. For the project narrative, API reference, and
`.env` setup, see `README.md`. For architecture and conventions, see `CLAUDE.md`. For the planned
Outbox + Prometheus work, see `docs/stateless-inventing-yao.md`.

## The three compose files (`docker_compose/`)

| File | Stack | What it brings up |
|---|---|---|
| `storages.yaml` | MongoDB | `mongodb` (port `27017`), `mongo-express` UI (port `28081`). |
| `kafka.yaml` | Kafka | `zookeeper` (`22181`), `kafka` (`29092`), `kafka-ui` (port `8090`). |
| `app.yaml` | FastAPI | `main-app` (port `${API_PORT}`→`8000`), built from `Dockerfile`, `depends_on` healthy `kafka`. |

`Makefile` targets compose these together (see `CLAUDE.md` → "Common commands"):

- `make all` — storages + kafka + app (everything).
- `make storages` / `make kafka` / `make app` — individual stacks.
- `make *-down` — tear each down (`all-down` for all).
- `make app-shell` (`docker exec -it main-app bash`), `make app-logs`, `make kafka-logs`.

## Port map (after `make all`)

| Service | URL |
|---|---|
| API docs | http://localhost:8000/api/docs |
| App metrics endpoint | http://localhost:8000/metrics |
| Prometheus server | http://localhost:9090 (port from `PROMETHEUS_PORT`) |
| Mongo Express | http://localhost:28081 |
| Kafka UI | http://localhost:8090 |
| MongoDB | `localhost:27017` |
| Kafka broker | `localhost:29092` (internal `kafka:29092`) |

## Inspection UIs

- **Mongo Express** (`28081`): browse documents in the chat database/collections.
- **Kafka UI** (`8090`): inspect topics, consumer groups, and messages. Useful to confirm events
  (`new-chats`, `new-message-received`, `chat-deleted`, `new-listener-added`) are published when you
  POST to the API.

## Two ways to run the app

### A. Docker (recommended for parity)

`make all` waits for Kafka health before starting `main-app`. The broker and connection manager are
started/stopped via the app lifespan (`app/application/api/lifespan.py` → `init_message_broker` /
`close_message_broker`). Note: the lifespan only **starts the broker**; it does **not** start a
consumer loop, so inbound Kafka→WebSocket relay is not active (see `docs/known-issues.md` #5).

### B. Local Poetry (no Docker)

```bash
poetry install
poetry run uvicorn --factory application.api.main:create_app --reload --host 0.0.0.0 --port 8000
```

Requires reachable Mongo + Kafka at the configured URLs (`Config` / `.env`). Run from a directory
where `app/` is importable (the app is launched with `app/` on the path via the `--factory`
`application.api.main:create_app` entrypoint).

## Running tests

Tests run against **in-memory repositories** (no Mongo/Kafka needed). Run from inside `app/` so the
flat first-party imports (`application`, `domain`, `logic`, `infrastructure`, `settings`, `test`)
resolve:

```bash
cd app
poetry run pytest          # or: pytest
```

The swap is done in `app/test/fixtures.py` (`init_dummy_container` rebinds `BaseChatsRepository` →
`MemoryChatRepository`) and applied via FastAPI `dependency_overrides` in
`app/test/application/api/conftest.py`. See `.claude/rules/testing-rules.md`.

> **Gotcha:** `init_dummy_container` only overrides `BaseChatsRepository`, not
> `BaseMessagesRepository`. Tests that need messages must use the memory variant too, or they'll hit
> real Mongo. See `docs/known-issues.md`.

## MongoDB: standalone vs replica set (important)

`storages.yaml` starts Mongo as a **standalone** (no `--replSet`). Consequences:

- Plain document writes work fine.
- **Multi-document MongoDB transactions fail** — they require a replica set. The planned Outbox
  pattern in `docs/stateless-inventing-yao.md` depends on converting this to a single-node replica
  set (`rs.initiate`). If you add `session`/`transaction`-based writes before that conversion, they
  will error. See `CLAUDE.md` → "Known gotchas".

## Common failure modes

| Symptom | Likely cause | Fix |
|---|---|---|
| App container won't start | Kafka not healthy yet | `make kafka` first, wait for healthcheck, then `make app`. |
| Tests fail with import errors | Not in `app/` dir, or venv not active | `cd app && poetry run pytest`. |
| Writes succeed but no Kafka messages | Broker down or topic mismatch | Check `kafka-ui`; verify `Config` topic/env vars. |
| `CommandHandlersNotRegisteredException` | Forgot `mediator.register_command` in `init_mediator` | Add the registration (see `docs/di-reference.md`). |

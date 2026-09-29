# Project Memory (Agent State)

> **Purpose:** single living snapshot of the project's architecture, recent changes, and open
> issues — so agents can skip re-scanning the repo each session. **Update this file after every
> meaningful change.** Where this file and older docs disagree, this file is newer — but re-verify
> line numbers before editing (files move).
>
> Last updated: **2026-09-28** · Tests: **333 passed, 0 warnings** (`cd app && uv run pytest`, ~4s) · mypy: **FULL STRICT clean, 140 files** (`uv run mypy` from repo root — files=["app"] is root-relative) · CI: GitHub Actions (`.github/workflows/ci.yml`: pre-commit lint, pytest via uv, mypy, promtool/amtool, docker build)

---

## 1. What this project is

FastAPI + Kafka + MongoDB + Valkey multi-user chat backend demonstrating DDD, CQRS, and
event-driven architecture. One demo user (OAuth2 password flow, JWT). Python 3.12
(`.python-version`), uv, pytest. Details: `docs/architecture.md`, `CLAUDE.md`.

---

## 2. Architecture (verified 2026-09-25)

```
HTTP/WS  application/api/{auth,messages}/handlers.py     (FastAPI routers)
              │ Depends(init_container) → punq container (app/logic/init.py, lru-cached)
              ▼
         logic/mediator/base.py (Mediator)  ── commands/queries/events, dispatch by class
              ▼
         logic/{commands,queries,events}/messages.py  (handlers)
              ▼
         infrastructure/repositories/messages/mongo.py  (Motor)
              ▼
         MongoDB (chats, messages, outbox collections)
              │  outbox pattern — the ONLY Kafka writer is the relay
              ▼
         infrastructure/outbox/relay.py (OutboxRelay, background task)
              ▼
         Kafka  ──►  consumer loop (application/api/lifespan.py)
              ▼
         NewMessageReceivedFromBrokerEvent → WebSocket fan-out (infrastructure/websockets/managers.py)
```

Key wiring facts:
- **DI single source of truth:** `app/logic/init.py` (`_init_container`). Full table: `docs/di-reference.md`.
- **Mediator is built last** by `build_mediator(container, config)`; command handlers are registered
  via *factories* (punq introspection quirk — do not "simplify" back to class registrations).
- **Tests** rebuild the mediator via `init_dummy_container` (`app/test/fixtures.py`), which overrides
  repos with in-memory versions and pops punq's cached singletons. Reuse this instead of ad-hoc mocking.
- **Transactions** via `_maybe_transaction` (Mongo session; test session provider returns `None`).

### Resilience layer (added 2026-09-25)
- `app/infrastructure/resilience.py`: `CircuitBreaker` (closed → open after N consecutive failures →
  half-open probe after `recovery_time` → closed). Proxies: `CircuitBreakerChatsRepository`,
  `CircuitBreakerMessagesRepository` wrap the Mongo repos in `init.py` — one shared `'mongo'` breaker.
- A second, **private** `'kafka'` breaker (same config knobs, NOT registered under the
  `CircuitBreaker` type — that key is the mongo one) guards the outbox relay's sends: open-state
  pre-check skips the batch, sends go through `breaker.call()`, `CircuitOpenError` mid-batch stops
  the batch without a histogram sample. `OutboxRelay.circuit_breaker=None` disables it (tests).
- `CircuitOpenError` → app-level handler in `application/api/main.py` → **HTTP 503 + Retry-After**
  (registered at app level on purpose: endpoints' broad `except ApplicationException → 400` would
  otherwise swallow it — do not add ApplicationException subclasses for infra errors).
- A third, **private** `'valkey'` breaker (ADR-0008) guards the cache client; it is swallowed
  INSIDE `ValkeyCacheClient`, because `CircuitOpenError` maps to HTTP 503 app-wide and a cache
  outage must never become an API outage.
- Config: `CIRCUIT_BREAKER_FAILURE_THRESHOLD` (5), `CIRCUIT_BREAKER_RECOVERY_TIME` (30s).
- Metrics: `circuit_breaker_state{name}`, `circuit_breaker_rejected_total{name}` (see `infrastructure/metrics.py`).

### Valkey (ADR-0008, implemented 2026-09-28)
- `app/infrastructure/cache/{base,valkey,memory,keys,cached}.py`, `presence/{base,valkey,memory}.py`,
  `locks/{base,valkey,memory}.py`; `docker_compose/valkey.yaml`; `valkey>=6.1.1,<7` added.
- **ONE file imports `valkey`: `infrastructure/cache/valkey.py`.** Everything else is behind ABCs.
  This forced a deviation from the ADR's own file list: the presence hash ops and the three lease
  primitives live on `BaseCacheClient`, so `ValkeyPresenceTracker`/`ValkeyLeaseLock` import no
  driver (ADR-0008 §9, deviation 1).
- **Wiring order: `CircuitBreaker(Cached(Mongo))`** — the cache proxy is INSIDE the breaker so a
  Valkey error is swallowed below it and never charged to the `'mongo'` breaker.
- **Versioned invalidation:** a post bumps `chat:ver:{oid}`, page keys embed the version; a delete
  drops `chat:{oid}` + the version key. No SCAN in the hot path.
- **`AddTelegramListenerCommandHandler` also invalidates** the chat detail entry (ADR-0008 §9,
  deviation 3 — the cached `Chat` carries listeners, so omitting this serves a stale listener list
  for a TTL).
- **Degradation is the contract:** every cache call returns the cold-cache value on error, logs a
  warning and counts `cache_errors_total` (PRE-breaker). An open `'valkey'` breaker is swallowed
  too — `CircuitOpenError` is mapped to HTTP 503 app-wide, and a cache outage must never become
  one. `set_if_absent` falls back to **False** ("could not acquire"), never True.
- **Presence:** `presence:{chat_oid}` hash, one field per socket, key TTL re-armed every
  ttl/3. Hash fields have no TTL in the Valkey 8 baseline, so a crashed socket's field dies with
  the key (over-count while survivors beat, never under-count). `GET /chat/{oid}/presence/`
  returns `enabled:false, count:0` when the flag is off.
- **Relay lease:** `OutboxRelay.lease: BaseDistributedLock | None`; acquire-or-skip at tick start
  (a follower never even reads Mongo), renew per tick, release in `run()`'s `finally` so shutdown
  hands the lease back. `lease=None` (default) is byte-for-byte the pre-ADR behavior.
- **All 7 knobs default OFF** in `Config()`; the compose `.env` is the only place they turn on.
  `create_cache_client` builds a `MemoryCacheClient` when all three flags are off, so no test or
  old deployment ever constructs a Valkey client.
- Gotchas: (1) **valkey-py types every command as `Union[Awaitable[T], T]`** (one class serves
  sync+async) so mypy cannot await it — the client field is `Any` at that ONE boundary (same
  pattern as aiokafka) and each method re-asserts its return type in a small closure; (2) `SET NX`
  returns **None**, not False, when the key exists — coerce with `bool()`; (3) `eval()` args are
  typed `str`, so pass `value.decode()` and `str(ttl)`; (4) `hash_set` takes `str` (not bytes)
  because valkey-py's stub demands it; (5) the WS heartbeat's done-callback LOGS but never COUNTS
  (the heartbeat counts its own failure) — counting in both double-reports.
- Verified: 333 passed, mypy clean, ruff clean, **live Valkey smoke** (`app/scripts/valkey_smoke.py`
  — 15/15 incl. both Lua scripts and the HSET+EXPIRE pipeline, since the test double cannot prove
  them), **live degradation drill** with the container stopped (every op returned its cold-cache
  value, breaker opened, nothing raised), `promtool check rules` 7 rules, compose config renders.
  Runbook: `docs/runbooks/valkey-outage.md`. Alert `CacheErrorsHigh` (warning — degradation, not
  an outage). Grafana gained 5 panels (lock stat, cache hit rate, ops by result, errors+acquisitions,
  heartbeat failures).

---

## 3. Conventions agents must follow

- **Flat imports only:** `application`, `domain`, `infrastructure`, `logic`, `settings` (no
  `app.` prefix, no relative imports). Domain imports must use the `domain` alias.
- **Metrics** live only in `app/infrastructure/metrics.py` (module-level, D1/D3/D5 rules of
  ADR-0006); update via `safe_inc`/`safe_set`/`safe_observe`. Bounded label sets only.
- **New handler recipe:** command/query class + handler in `logic/{commands,queries}/messages.py`,
  factory registration in `_init_container`, wire in `build_mediator`, add tests
  (see `docs/cqrs-contract.md`).
- **Formatting:** ruff line-length 100, single quotes, **tabs** in some files / **4 spaces** in
  others — match the file you are editing, do not reformat.
- **After code changes:** `cd app && uv run pytest` (all 333 must stay green).

---

## 4. Recently completed (newest first)

- **2026-09-28 — `init-kafka` one-shot service: Kafka topics are now pre-created (no more
  auto-create warning spam).** The app only produces/consumes, so on a cold broker every first
  send/subscribe raced the broker's auto-create: aiokafka logged
  `Topic new-messages|new-chats-topic is not available during auto-create initialization`
  (`aiokafka/cluster.py:256`, `LeaderNotAvailableError`) plus a roundrobin
  `No partition metadata for topic` until the topic materialized — benign, but a wall of noise
  (and the reason the `POST /chat/` timestamp was followed by 4 warnings 100 ms apart).
  `docker_compose/kafka.yaml` gains `init-kafka`, modelled on `init-mongo` (`storages.yaml:19`):
  `restart: 'no'`, list-form command + block scalar, `kafka-topics --create --if-not-exists` for
  the 4 topics, a `seq 1 30` retry loop (the `nc -z` healthcheck only proves the port is open,
  not that topic requests succeed yet), topic names from env with `:-` defaults mirroring
  `settings/config.py`. `app.yaml` now depends on it with `service_completed_successfully`.
  Verified: creates the 2 missing topics, exit 0, all 4 present; second run is a no-op (exit 0).
  No app code changed — the contract is identical, only the timing noise is gone.

- **2026-09-28 — ADR-0008: Valkey for cache-aside, presence, and the outbox relay lock
  (Accepted; all 6 chunks implemented).** `docs/adr/0008-valkey-cache-presence-lock.md` §9 has the
  status table plus 7 recorded deviations from the plan. `app/infrastructure/cache/` (base/valkey/
  memory/keys/cached), `presence/`, `locks/`; `infrastructure/cache/valkey.py` is the only file
  importing the driver. `CACHE_ENABLED` / `PRESENCE_ENABLED` / `RELAY_LOCK_ENABLED` default OFF
  in `Config()`, ON in the compose `.env`. `noeviction` (128 MiB) so the relay lock can never be
  silently evicted. New metrics `cache_operations_total{operation,result}`,
  `cache_errors_total{operation}`, `presence_heartbeat_failures_total`,
  `outbox_relay_lock_acquired_total`, `outbox_relay_lock_held`; a third breaker name `'valkey'`.
  New endpoint `GET /chat/{oid}/presence/`. Test count 217 → 333. Details in the "Valkey
  (ADR-0008)" section above.
- **2026-09-28 — ADR-0008: Valkey adoption plan (Proposed, no code yet).**
  `docs/adr/0008-valkey-cache-presence-lock.md` — decision: adopt Valkey for three scoped features:
  cache-aside for the query path (versioned invalidation, no SCAN), WS presence (hash + TTL
  heartbeat), outbox relay leader lock (SET NX PX lease). NOT for: Kafka replacement, WS fan-out,
  durable storage. Six chunks (compose+client infra, read cache, invalidation, presence, lock,
  observability/docs); all feature flags default OFF in `Config()` so tests/single-replica deploys
  are untouched; `noeviction` (lock must not be evicted); client = `valkey` (valkey-py), the only
  file importing it is `infrastructure/cache/valkey.py`, mypy-strict caveat noted (aiokafka
  pattern). Status table in ADR §9 — all chunks Not started.
- **2026-09-28 — mypy wired into CI + as a CD deploy gate.** `.github/workflows/ci.yml`: new
  `typecheck` job (name `Type check (mypy)`) — same uv pattern as tests (setup-uv@v10.2.0 pinned
  0.12.19, `uv sync --frozen` so CI runs exactly the locked mypy 1.20.2), plus a rolling
  `.mypy_cache` cache (actions/cache@v4, key `mypy-<os>-<run_id>`, restore-keys prefix — the mypy
  docs recipe). Runs from the REPO ROOT (no working-directory): `[tool.mypy].files=["app"]` is
  resolved relative to pyproject.toml. CI is now 5 jobs: lint, tests, typecheck, configs,
  docker-build. `.github/workflows/cd.yml`: a second `wait-on-check-action` gate for
  `Type check (mypy)` alongside `Tests (pytest)` — check-name must match the job NAME exactly.
  README CI/CD story synced (5 jobs, both gates). Committed separately from the strict migration
  (already landed as 6e7180a). YAML structure validated via yaml.safe_load; pre-commit hooks skip
  YAML (pyupgrade/ruff file filters).
- **2026-09-28 — mypy switched to FULL `strict = true` (local-only, all 509 initial errors fixed at the root).**
  pyproject `[tool.mypy]`: `strict = true` + `disallow_any_generics`/`disallow_subclassing_any`/
  `disallow_any_unimported` (user decision: NO test overrides — tests fully annotated too). 509
  errors in 59 files → 0. Key fixes: **motor generics parameterized** `AgnosticClient[AgnosticCollection][dict[str, Any]]`,
  `AsyncIOMotorClient[dict[str, Any]]` (motor ships .pyi stubs, is Generic); **aiokafka has no stubs** →
  `disallow_any_unimported` forbids its names in annotations, so `KafkaMessageBroker.producer/consumer`
  are typed `Any` at that single boundary (comment explains); **kafka.stop_consuming made SYNC** `-> None`
  (was `async def` — never matched the ABC; 2 tests updated to plain calls); `breaker.call()` generic
  over `_T` (kills the proxies' Any-returns); breaker proxies' `inner` typed as the repo ABCs (not Any);
  repo `session=None` params typed (`SessionHint=Any` in messages/base.py, real
  `AsyncIOMotorClientSession` in mongo repos); `EventMediator` imported from `logic.mediator.event`,
  `NewMessageReceivedFromBrokerEvent` from `domain.events.messages` (strict no_implicit_reexport);
  `BaseCommand`/`CommandHandler._mediator` typed `EventMediator[BaseEvent, Any]`; `lifespan` ->
  `AsyncIterator[None]`, tasks `asyncio.Task[None]`; `encode_token/create_token` -> `dict[str, Any]`,
  `verify_token` runtime-checks `sub` is str; `PrometheusFastApiInstrumentator` imported from
  `.instrumentation` (same re-export issue). Test files: fake containers/streams annotated, async
  generators fixed to sync-def-returning-iterator pattern (mypy async-iterator guidance), duck-typed
  repo stubs bridged with `cast()` + one `# type: ignore[attr-defined]`/`[call-overload]` each for
  punq `_singletons` internals and module patching (noqa B010 where setattr). GOTCHAS: (1) `uv run
  mypy` must run from REPO ROOT (files=["app"]); (2) str_replace with old strings spanning a
  line-boundary comment can swallow a newline — always re-read edited hunks; (3) ruff RUF100 moved
  noqa TRY004 must sit on the `raise` line, not the `if`. Verified: mypy clean (121 files), 217
  passed, ruff clean on all 60 changed files, `uv lock --check` fresh, `create_app()` boots.
- **2026-09-28 — Migration Poetry → uv (local + Dockerfile + CI).** pyproject: `[tool.poetry]`
  package-mode block dropped; NO `[build-system]` (application repo — `uv sync` manages deps only);
  `.python-version` = `3.12` added (the single pin uv reads everywhere). `uv.lock` (74 pkgs)
  replaces `poetry.lock` (deleted); `.venv/` gitignored. Verified locally: 217 passed, mypy clean
  (121 files), ruff clean, `uv lock --check` fresh (local uv 0.12.7; cross-checked with pinned
  0.12.19). **Dockerfile rewritten** to the uv pattern: builder
  `ghcr.io/astral-sh/uv:python3.12-bookworm-slim` runs `uv lock --check`, then builds `/opt/venv`
  (prod, `--no-dev`) and `/opt/venv-dev` (`--all-groups`) with `UV_COMPILE_BYTECODE=1` + BuildKit
  cache mounts. **Stages reordered: prod is LAST** — compose and docker/build-push-action build the
  final stage by default, and the old file ended on dev, so CI/GHCR were actually shipping the DEV
  image; prod is now non-root (`appuser`) with a bare-run CMD. Venvs live OUTSIDE `/app` on purpose:
  dev compose bind-mounts `../app/` over `/app` and would hide a venv inside the workdir; dev stage
  is selected via new `target: dev` in `docker_compose/app.yaml`. **CI tests job**: setup-uv@v10.2.0
  (version 0.12.19, enable-cache) + `uv sync --frozen` + `uv run --frozen pytest -q`; its
  actions/setup-python was removed (uv provisions 3.12 from `.python-version` — same source as
  Docker/local). Lint job unchanged; cd.yml untouched (builds the Dockerfile, no pkg manager).
  Docker build + smoke test VERIFIED locally (2026-09-28, daemon up): both targets built, uv boot
  + /api/docs + /metrics 200 in dev AND prod (prod via the image's own CMD, non-root, against
  real mongo/kafka on the compose `backend` network); `uv lock --check` also cross-checked inside
  the exact builder image (uv 0.12.19/CPython 3.12.12). **COPY gotcha hit live:** the old
  `COPY /app/ /app/**` copied app/'s CHILDREN into /app — the natural rewrite `COPY ./app ./app`
  nests code at /app/app/ and flat imports break; correct form is `COPY ./app/ ./`. Windows Git
  Bash gotcha: `docker run -v /src` gets mangled to C:/Program Files/Git/src — use
  `MSYS_NO_PATHCONV=1` + `$(pwd -W)`. Docs switched to `uv run`: README, local-development,
  user-guide, architecture, agents.md; historical ADR/plan
  docs left as-is. Gotchas: (1) uv defaults to the newest interpreter — without `.python-version`
  it picked local 3.14; (2) keep generated files ASCII — em-dashes broke two write attempts.
- **2026-09-28 — mypy added (pragmatic baseline, LOCAL-ONLY enforcement).** Dev dep `mypy@^1.18.2`
  (resolved 1.20.2) + `[tool.mypy]` in pyproject: pydantic plugin, `check_untyped_defs`,
  `no_implicit_optional`, `warn_unused_ignores/configs`, `files=["app"]`; `ignore_missing_imports`
  overrides for punq/aiokafka (no stubs). Deliberately NOT in pre-commit/CI yet (user decision —
  revisit when tightening). First run 103 errors in 27 files → **0 fixed at the root** (only
  deliberate-ABC tests carry `type: ignore[abstract]`). Key structural fixes: mediator made generic
  (`EventMediator[ET, ER]` / `QueryMediator` / `CommandMediator`; maps are class-keyed
  `dict[type[...], list[...]]`; concrete `Mediator` parameterized `[Base*, Any]` with `Any`-valued
  handler lists — invariance); command/query DTOs inherit plain (non-dataclass) marker classes
  `BaseCommand`/`BaseQuery` (they could NOT inherit the old frozen dataclasses — TypeError);
  concrete handlers parameterized (`CommandHandler[Cmd, Result]`, `BaseQueryHandler[Q, R]`,
  `EventHandler[E, Any]`); repo bases aligned to implementations (`get_all_chats(filters) ->
  tuple[list[Chat], int]`, infra owns its filters — the old base imported the API pydantic ones);
  motor `_collection`/outbox `collection` typed `AgnosticCollection`; broker ABC
  `start_consuming`/`stop_consuming` now SYNC returning `AsyncIterator[dict]` (matches every
  impl and the consumer loop); breaker proxies subclass the repo bases; `Chat.__eq__(object)`
  LSP fix; relay send wrapped in typed closure `_send_operation(row)`. Dead code deleted:
  duplicated query classes + `GetMessagesQueryHandler` in logic/commands/messages.py (called a
  nonexistent repo method) and `MemoryChatRepository.find_chats_by_user_id` (read a nonexistent
  `chat.user_id`). GOTCHAS: (1) PEP 696 `default=` TypeVars type-check under mypy but **raise
  TypeError on Python ≤3.12** — do not use until the floor is 3.13; (2) `GetAllChatsQueryHandler`'s
  old `# type: ignore` hid that it returns the (chats, count) TUPLE the API unpacks — annotation
  now honest; (3) lock regenerated via the Poetry 2.3.0 throwaway venv (`/tmp/p230`, Windows:
  `Scripts/` not `bin/`); local 2.1.3 still fine for `poetry run`. Verified: mypy clean, ruff
  clean on all changed files, 217 passed, `poetry check` clean, plain `poetry run mypy` works.
- **2026-09-27 — All dependencies bumped to latest.** pyproject constraints + lock regenerated
  (throwaway Poetry 2.3.0 venv; the 1.8.2 rule is obsolete, 2.1.3 can't do PEP 735 locks).
  Notables: fastapi 0.115→0.141 (starlette 0.40→**1.7**), aiokafka 0.10→0.14, punq 0.7→0.9,
  pytest 8→9, pytest-asyncio 0.24→1.4 (explicit `@pytest.mark.asyncio` markers still fine —
  no `asyncio_mode` config exists), ruff 0.15.22→**0.16.9** (default rules 59→413 — adopted;
  see the lint bullet below), pre-commit ruff hook rev bumped to v0.16.9 to match. Verified:
  217 passed, `ruff check .` clean, pre-commit `--all-files` idempotent, `poetry check` clean.
  Starlette 1.7's testclient deprecation warning surfaced by this bump — resolved separately,
  see the httpx2 bullet below.
- **2026-09-27 — httpx2 added to dev deps (starlette TestClient deprecation resolved).**
  Starlette ≥1.2's TestClient prefers `httpx2` (Pydantic-maintained httpx rename, same API)
  and warns on the plain-`httpx` fallback; the fallback is removed in starlette 2.0. Fix:
  `httpx2>=2.13.1,<3` in the dev group + `test/application/api/test_messages.py` annotation
  re-typed to `httpx2.Response` (TestClient now returns httpx2 objects). Runtime httpx
  deliberately STAYS: the Telegram notification client, lifespan.py and fastapi[httpx] still
  need it; the two packages coexist by design (starlette's own `full` extra ships both, and
  httpcore 1.0.9 + httpcore2 2.13.1 share h11 0.16 — no resolver conflict). Verified: 217
  passed with zero warnings, ruff clean, pre-commit idempotent, `poetry check` clean.
  Future trigger: when fastapi flips its extras to httpx2 (its own test suite already uses
  it), migrate runtime httpx → httpx2 and drop the old package.
- **2026-09-27 — Lint stack: ruff owns import sorting; isort hook removed.** ruff 0.16's default
  rule set adopted (no rollback to `E4/E7/E9/F`). Config added under `[tool.ruff.lint]`:
  `flake8-bugbear.extend-immutable-calls = ["fastapi.Depends"]` (B008), `isort.known-first-party`
  = the flat aliases, `per-file-ignores` (B008/DTZ in 3 test files). **Gotcha:** per-file-ignores
  globs resolve from the pyproject.toml dir (repo root) — they need the `app/` prefix.
  `[tool.isort]` deleted from pyproject + isort hook dropped from pre-commit (ruff's I001 can't
  reproduce its custom FASTAPI section — the two would fight forever). 96 auto-fixes applied
  (import order, UP035/045/017, RUF100); manual: 4 deliberate `BLE001` noqa (best-effort
  boundaries), relay lambda binds `row=row` (B023), `__eq__(self, value, /)` (PYI063), plain
  `import orjson` (PLC0414).
- **2026-09-25 — PEP 621 migration done (branch `refactor/pep621`, own PR).** `pyproject.toml`
  now uses `[project]` + PEP 735 `[dependency-groups]` (dev) + `[tool.poetry] package-mode=false`
  (application — replaces CI's `--no-root`); `[build-system]` dropped. **Poetry pin bumped
  1.8.2 → 2.3.0** in Dockerfile (+ `poetry-plugin-export` — export is a plugin since 2.x) and CI.
  Gotchas, all hit live: (1) PEP 735 groups need Poetry ≥2.2, and the 2.2 lock-hash bug makes
  **2.3.0 the minimum safe pin** — 2.1.3 silently ignores `[dependency-groups]`; (2) `punq`
  declares `Requires-Python <4.0`, so `requires-python` MUST carry the same ceiling
  (`">=3.11,<4.0"`) — an open range fails resolution; (3) local Poetry 2.1.3 is now OLDER than
  the 2.3.0 pin — regenerate locks with 2.3.0 (throwaway venv `/tmp/p230` pattern or upgrade
  local Poetry), the old "use 1.8.2" rule above is OBSOLETE. Verified: `poetry check` clean
  (deprecation warnings gone), lock has dev group, prod export 31 pkgs w/o ipython, dev export
  64 pkgs, fresh-venv `poetry install --with=dev` + **217 passed** + ruff clean. Docker builder
  stage not built locally (daemon down) — verified by the PR's docker-build job.
- **2026-09-25 — Repo polish: MIT LICENSE + ipython out of the prod image.** Added `LICENSE`
  (MIT, © M0ng00se7169); moved `ipython` from runtime deps to the dev group (it was shipped into
  the prod image via the Dockerfile's `poetry export`) and filled the empty pyproject
  description. **Gotcha that almost shipped:** local Poetry is 2.1.3 — its lock file
  (lock-version 2.1) is unreadable by the Poetry 1.8.2 pinned in the Dockerfile, and 2.x dropped
  the built-in `export` the Dockerfile uses. Lock was regenerated with 1.8.2 in a throwaway venv
  (`python -m venv` + pip install poetry==1.8.2 → `poetry lock --no-update`); verified prod
  export has 0 ipython hits, dev export has 1, 217 tests pass. **Rule: never run bare
  `poetry lock` with the local 2.x — regenerate with 1.8.2.**
- **2026-09-25 — README portfolio section: "Engineering Highlights".** Top-of-README 4-step
  CI/CD story table (push/PR → merge → deploy → rollback) linking the CI/CD run pages, workflow
  files, GHCR packages, the deploy-and-rollback runbook and ADR-0006/0007; H1 title + CD badge
  added next to the CI badge. The lower "CI/CD" section slimmed to "CI/CD (details)" (operational
  reference only) — job tables live once, in Highlights.
- **2026-09-25 — Deploy + rollback path documented.** `docs/runbooks/deploy-and-rollback.md`:
  run the stack from CD's GHCR images and roll back by pinning
  `APP_IMAGE=ghcr.io/m0ng00se7169/routerelay:<sha>` in `.env` (never git-revert to roll back —
  that triggers a fresh CD build). New `deploy/compose/docker-compose.deploy.yml` REPLACES
  `main-app` (compose `-f` override merge cannot remove keys — an `image:` override over
  `app.yaml` would still hit its `build:` and repo bind-mount + `--reload`; verified via
  `docker compose config`: no build/volumes in the rendered merge, APP_IMAGE resolves).
  Blast-radius table: Mongo/Kafka/AM state survives an app swap; the app is stateless by design
  (outbox). `.github/workflows/cd.yml`: on push to `main` → wait for
  the CI run's `Tests (pytest)` check on the same SHA (`lewagon/wait-on-check-action`) → buildx
  build + push to `ghcr.io/m0ng00se7169/routerelay` (tags: `latest` on the default branch +
  full commit SHA via metadata-action; GHA layer cache). Auth is the built-in `GITHUB_TOKEN`
  with `packages: write` — no PAT; package visibility is flipped per-package on GHCR after the
  first push (independent of repo visibility). CI's `docker-build` job now skips on `main`
  (`if: github.ref != 'refs/heads/main'`) so the image builds exactly once per SHA.
- **2026-09-25 — CI pipeline added (GitHub Actions).** `.github/workflows/ci.yml`, 4 jobs:
  lint = the repo's own pre-commit suite (`--all-files`, pyupgrade/ruff/add-trailing-comma/isort —
  `ruff format` is deliberately NOT a gate, 83 files would be reformatted), tests = pytest
  (self-contained, no services), configs = promtool + amtool validation of the observability
  stack, docker build = Buildx image build check with GHA cache. Triggers: push to
  `main`/`features`, PRs to `main`. Enabling it required a lint baseline: 2 unused imports
  removed + isort normalized 5 JWT-import blocks (pre-commit suite is now idempotent on all
  files). README gained the CI badge + a CI/CD section; README/architecture.md de-duplicated
  first (Makefile table corrected to all 17 real targets — the old architecture.md copy said
  "Postgres", and the tech table claimed SQLAlchemy/Loguru which this stack never had).
  Not yet pushed; first Actions run happens on the next push.
- **2026-09-25 — Telegram alerting live (ADR-0007 §4 decision).** `oncall-critical` and
  `team-warnings` in `docker_compose/alertmanager/alertmanager.yml` gained `telegram_configs`
  (AM's built-in receiver, v0.34.1) while KEEPING the webhook sink — dual delivery. Decided
  against an app-relay endpoint: keeps paging alive when the app itself is down, zero app code,
  AM owns retries. Secrets: token/chat id in **gitignored** files
  (`docker_compose/alertmanager/secrets/telegram_bot_token|telegram_chat_id`), mounted as compose
  file-secrets to `/run/secrets/`, read via `bot_token_file`/`chat_id_file` — AM config cannot
  expand env vars; `.env.example` documents the names only. Verified live: amtool SUCCESS, routes
  unchanged, temp critical+warning rules → Telegram messages received (AM
  `alertmanager_notifications_total{integration="telegram"}` 1→2, zero errors; critical
  immediate, warning after 5m group_wait) + sink CRITICAL/WARNING lines; test rules removed,
  alerts file back to 6 rules, AM restart clean.
- **2026-09-25 — ADR-0007 Chunk 4: runbook_url annotations complete (6/6 alerts).** The 5
  previously bare alerts in `docker_compose/prometheus-alerts.yml` gained `runbook_url`:
  `OutboxRelayFailing`/`OutboxBacklogGrowing` → existing `docs/runbooks/kafka-outage.md`; the
  consumer pair → new `docs/runbooks/kafka-consumer.md` (documents that `kafka_consumer_up` stays 1
  through reconnect backoff and drops only on non-cancelled task death; `KAFKA_CONSUMER_BACKOFF_*`
  knobs; consumer restart = app restart, messages retained in the topic); `WSBroadcastFailures` →
  new `docs/runbooks/ws-fanout.md` (best-effort fan-out, one dead socket no longer aborts the
  broadcast, failures = client instability, no app restart). Validated: promtool 6 rules SUCCESS,
  `kill -HUP prometheus`, all 6 `health: ok` with runbook_url via `/api/v1/rules`. Sink appends
  runbook_url to log lines automatically. ADR-0007 §5/§9 + architecture.md + README synced.
  No code changes; test count unchanged (217).
- **2026-09-25 — ADR-0007 Chunks 1-3 implemented (webhook-sink transport, no external creds).**
  Alertmanager runs in the stack (`docker_compose/alertmanager.yaml` +
  `alertmanager/alertmanager.yml`, `ALERTMANAGER_PORT=9093` in both env files, make targets
  extended symmetrically); prometheus.yml gained the `alerting:` peer + an `alertmanager`
  self-monitoring job. Routing: severity-asymmetric (critical 0s wait/1h repeat, warnings 5m/12h),
  inhibition pairs from the drill, all receivers → `POST /ops/alerts` webhook sink on the app
  (`application/api/ops/handlers.py`: severity→log level, resolved=INFO, runbook appended; 6 new
  tests; suite 211 → 217). Verified: amtool check-config + routes test (all 3 paths), promtool
  clean, live E2E — temp critical rule → AM active → CRITICAL line in the app JSON logs.
  Telegram transport stays available: swap receiver URLs only. Silencing procedure added to the
  kafka-outage runbook. ADR-0007 §9 records the implementation status + deviations.
- **2026-09-25 — ADR-0007: Alertmanager wiring plan.** `docs/adr/0007-alertmanager-wiring.md`
  (Proposed): closes the follow-up deferred since ADR-0006 Chunk 7.2. Four chunks — (1) run
  Alertmanager in the stack (compose file, `alerting:` section in prometheus.yml, make targets,
  `ALERTMANAGER_PORT=9093` in both env files); (2) routing tree: critical pages immediately,
  warnings grouped per subsystem, **inhibition pairs from the drill** (CircuitOpen mutes
  RelayFailing; ConsumerDown mutes Reconnecting); (3) receiver transport — **recommendation:
  Telegram via webhook relay (open decision, ask before provisioning)**; (4) hygiene — fill the
  five missing `runbook_url` annotations + document silences (`amtool silences add` for drills).
  Cross-referenced from architecture.md/README deferral notes. No code changes yet.
- **2026-09-25 — Grafana breaker panels.** `kafka-chat-overview` gained a new row after the outbox
  errors panels: a `stat` panel (breaker state per `name`, mapped 0=closed / 1=open-or-half-open,
  red threshold at open) and a timeseries of `sum(increase(circuit_breaker_rejected_total[15m]))
  by (name)` — rejections by breaker, which is exactly the alert's expr. Lower panels shifted down
  a row; dashboard `version` bumped to 2. Verified live: provisioned on `docker restart grafana`,
  both panel queries return data through the datasource API. Grafana 11 datasource queries go
  through `/api/datasources/uid/<uid>/resources/...` (the old `/proxy` path is gone). Port note:
  the container had bound an **ephemeral host port** (GRAFANA_PORT unset at creation → Docker
  random); fixed by adding `GRAFANA_PORT=3000` to `.env` (matching the tracked `.env.example`)
  and recreating the container — UI now stably at `localhost:3000`.
- **2026-09-25 — Kafka outage runbook.** `docs/runbooks/kafka-outage.md`: triage + recovery built
  from the drill's observed timeline (detection latency ≈ aiokafka 40s request timeout × threshold
  5 ≈ 3.5 min; errors freeze once the breaker opens — `OutboxRelayFailing` quiet while
  `OutboxRelayCircuitOpen` fires; recovery needs no app restart; alert clears ~15m after the last
  rejection). Wired via `runbook_url` annotation on the alert (validated with promtool, hot-loaded
  into the running Prometheus, `health: ok` after reload); linked from the architecture alert table.
- **2026-09-25 — Producer durability: `acks=all`.** `KafkaMessageBroker` gained an `acks` field
  (default `'all'`, passed to `AIOKafkaProducer`). With sends now awaiting the ack (see the drill
  fix below), the ack requires every in-sync replica instead of aiokafka's default `acks=1`.
  Unchanged on the single-broker dev cluster (ISR is just the leader; verified live — message read
  back from the topic log, relay drain clean, breaker closed). Test count unchanged (211).
- **2026-09-25 — Live fire drill found+fixed: producer sends were not awaiting the ack.**
  `KafkaMessageBroker.send_message` now uses `producer.send_and_wait()` (was `send()`, which only
  buffers and returns a delivery future): broker failures never surfaced, outbox rows were marked
  sent against a DEAD Kafka (at-most-once, buffered rows lost on restart), and the `'kafka'`
  breaker had no failures to count. Regression test pins the contract
  (`test/infrastructure/test_lifespan_outbox_broker.py`). Verified end-to-end in the live stack:
  breaker opened after 5 consecutive real failures, `OutboxRelayCircuitOpen` fired in Prometheus,
  Kafka restart → half-open probe closed the breaker and drained the outbox. Note: the alert's
  expr only sees data once a breaker opens (labelled series are created lazily).
- **2026-09-25 — Alert on relay breaker rejections.** New `OutboxRelayCircuitOpen` (warning) in
  `docker_compose/prometheus-alerts.yml`:
  `increase(circuit_breaker_rejected_total{name='kafka'}[15m]) > 0` — fills the gap where
  `OutboxRelayFailing` goes quiet precisely because the breaker stopped the doomed sends
  (gate rejections only land in the breaker counter). Alert count 5 → 6. Synced:
  `docs/architecture.md` (added the missing `circuit_breaker_state/rejected_total` registry rows
  + alert table), `README.md` (metric list + alert count), `infrastructure/metrics.py` comment
  (name label now documents 'mongo' + 'kafka'). `promtool check config` executed (Docker up):
  1 rule file / 6 rules — also closes the 'promtool pending' caveat on the reconnect-alert entry.
- **2026-09-25 — O-2 fixed: outbox relay guarded by a `'kafka'` circuit breaker.**
  `infrastructure/outbox/relay.py` (optional `circuit_breaker` field; open-state pre-check skips
  the batch — rows stay unsent; sends wrapped in `breaker.call()`; `CircuitOpenError` mid-batch
  skips the histogram sample — a rejection is not an attempt), `logic/init.py`
  (`create_outbox_relay` builds the private `'kafka'` breaker from the shared config knobs),
  tests `test/infrastructure/outbox/test_relay.py` (+6: trip, fail-fast skip, mid-batch trip,
  recovery resume, back-compat). Gotcha: `call()`'s success-reset wipes an externally-set open
  state — breakers can only be tripped by their own failed operations. Test count 206 → 211.
- **2026-09-25 — Circuit breaker for the Mongo path.** Files: `infrastructure/resilience.py` (new),
  `logic/init.py`, `application/api/main.py`, `settings/config.py`, `infrastructure/metrics.py`,
  `test/fixtures.py` (opt-in `wrap_repos_with_breaker=True`), tests
  (`test/infrastructure/test_resilience.py`, `test/application/api/test_circuit_breaker_503.py`),
  `docs/di-reference.md`. Test count 191 → 202.
  Regression covered: failed half-open probe must reopen the breaker even when
  `failure_threshold > 1` (otherwise it deadlocks rejecting forever).
- **2026-09-25 — O-3 fixed: ruff added to poetry dev deps.** `ruff@^0.15.22` (pinned to match the
  pre-commit hook) — `poetry run ruff check` now works as documented in `.pi/APPEND_SYSTEM.md`.
  Side effect: poetry reconciled the venv to `poetry.lock` (several locally-newer packages
  downgraded); full suite re-verified after. Ruff clean on all session-touched files (caught one
  unused import, fixed). Stray root `uv.lock` (`requires-python >=3.14`, unreferenced by any
  tooling) deleted 2026-09-25 after user review.
- **2026-09-25 — Alert on consumer reconnects.** New `KafkaConsumerReconnecting` (warning) in
  `docker_compose/prometheus-alerts.yml`: `increase(kafka_consumer_reconnects_total[15m]) > 0` —
  recency signal for broker stream death; reconnects self-heal, consumer death stays
  `KafkaConsumerDown`'s (critical) job. Alert count 4 → 5. Synced: `docs/architecture.md`
  (metric + alert tables), `README.md` (metric list + alert count). Validated structurally;
  `promtool` validation completed 2026-09-25 once Docker was available (see the breaker-alert
  entry above).
- **2026-09-25 — O-1 fixed: Kafka consumer reconnect + heartbeat.** `_kafka_consumer_loop`
  (`application/api/lifespan.py`) now wraps the stream in a reconnect loop with exponential backoff
  (initial 1s, cap 30s; `KAFKA_CONSUMER_BACKOFF_*` config; backoff resets on received messages).
  Both crash AND clean stream exits trigger reconnect (`kafka_consumer_reconnects_total{topic}`);
  only cancellation propagates. Heartbeat: `kafka_consumer_up` now drops via done-callback on ANY
  non-cancelled completion; it stays 1 through reconnect backoff (watch reconnects_total).
  Test count 202 → 206.
- **2026-09-25 — `docs/known-issues.md` rewritten.** All 8 stale entries verified fixed and moved
  to a status index; 3 verified open issues filed (O-1 Kafka reconnect + heartbeat gap, O-2 relay
  breaker, O-3 ruff missing from poetry env). Corrections found while verifying: the pre-commit
  lint gate exists (earlier "no lint gate" note was wrong); `BaseConnectionManager` is registered
  exactly once.

---

## 5. Open issues (verified against source on 2026-09-25)

`docs/known-issues.md` was rewritten 2026-09-25 and now matches reality — it holds a one-line
index of the formerly-listed issues. **No open issues remain:** all 8 legacy entries plus O-1
(Kafka consumer reconnect + heartbeat), O-2 (relay `'kafka'` breaker) and O-3 (ruff in poetry env)
are verified fixed as of 2026-09-25.

- **2026-09-28 — fresh-clone drill fixed `init-mongo` (was broken for FIRST-TIME users).**
  Simulated a fresh `git clone` (repo copied to /tmp minus local junk, `.env.example` → `.env`)
  and ran the README flow. The uv-migrated Dockerfile/compose worked, but `storages.yaml`'s
  init-mongo died with `syntax error near unexpected token '&&'` (nested YAML/bash quoting) —
  pre-existing, invisible locally because the old `docker_compose_dbdata6` volume was already
  initiated. On a truly fresh volume the replica set never came up → transactions/outbox dead.
  Fixed with list-form command + block scalar, idempotent (skips when `rs.status().ok == 1`).
  Verified BOTH paths on a throwaway compose project (`-p frest` → fresh volume): fresh volume
  initiates + PRIMARY + transaction roundtrip; already-initiated volume skips cleanly (exit 0).
  Also demystified: aiokafka `GroupCoordinatorNotAvailableError` spam on cold boot is benign
  (auto-create lag; settles in ~1 min, `kafka_consumer_up 1`, topic `new-messages` created) —
  and is now GONE: the sibling warning `Topic X is not available during auto-create
  initialization` is fixed at the source by `init-kafka` (see §4, 2026-09-28).
  Also: OpenAPI JSON lives at `/openapi.json` (only docs_url is under `/api`). Compose project
  name comes from the FIRST `-f` file's dir — both copies get `docker_compose`, so volumes are
  shared across checkouts (use `-p` to isolate).

---

## 6. How to update this file

After any meaningful change, edit **section 4** (one bullet: date, one-line what, files touched)
and **section 5** (add/remove issues), and refresh the "Last updated" + test-count line at the top.
Keep the file under ~150 lines — it is a snapshot, not a changelog. Full history lives in git.

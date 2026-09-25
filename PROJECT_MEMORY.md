# Project Memory (Agent State)

> **Purpose:** single living snapshot of the project's architecture, recent changes, and open
> issues — so agents can skip re-scanning the repo each session. **Update this file after every
> meaningful change.** Where this file and older docs disagree, this file is newer — but re-verify
> line numbers before editing (files move).
>
> Last updated: **2026-09-25** · Tests: **206 passed** (`cd app && poetry run pytest`, ~2.5s)

---

## 1. What this project is

FastAPI + Kafka + MongoDB multi-user chat backend demonstrating DDD, CQRS, and event-driven
architecture. One demo user (OAuth2 password flow, JWT). Python 3.11+, Poetry, pytest.
Details: `docs/architecture.md`, `CLAUDE.md`.

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
  via *factories* (punq 0.7.0 introspection quirk — do not "simplify" back to class registrations).
- **Tests** rebuild the mediator via `init_dummy_container` (`app/test/fixtures.py`), which overrides
  repos with in-memory versions and pops punq's cached singletons. Reuse this instead of ad-hoc mocking.
- **Transactions** via `_maybe_transaction` (Mongo session; test session provider returns `None`).

### Resilience layer (added 2026-09-25)
- `app/infrastructure/resilience.py`: `CircuitBreaker` (closed → open after N consecutive failures →
  half-open probe after `recovery_time` → closed). Proxies: `CircuitBreakerChatsRepository`,
  `CircuitBreakerMessagesRepository` wrap the Mongo repos in `init.py` — one shared `'mongo'` breaker.
- `CircuitOpenError` → app-level handler in `application/api/main.py` → **HTTP 503 + Retry-After**
  (registered at app level on purpose: endpoints' broad `except ApplicationException → 400` would
  otherwise swallow it — do not add ApplicationException subclasses for infra errors).
- Config: `CIRCUIT_BREAKER_FAILURE_THRESHOLD` (5), `CIRCUIT_BREAKER_RECOVERY_TIME` (30s).
- Metrics: `circuit_breaker_state{name}`, `circuit_breaker_rejected_total{name}` (see `infrastructure/metrics.py`).

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
- **After code changes:** `cd app && poetry run pytest` (all 202 must stay green).

---

## 4. Recently completed (newest first)

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
  unused import, fixed). Remaining stray: root `uv.lock` (`requires-python >=3.14`) looks
  abandoned — candidates for deletion after user review.
- **2026-09-25 — Alert on consumer reconnects.** New `KafkaConsumerReconnecting` (warning) in
  `docker_compose/prometheus-alerts.yml`: `increase(kafka_consumer_reconnects_total[15m]) > 0` —
  recency signal for broker stream death; reconnects self-heal, consumer death stays
  `KafkaConsumerDown`'s (critical) job. Alert count 4 → 5. Synced: `docs/architecture.md`
  (metric + alert tables), `README.md` (metric list + alert count). Validated structurally
  (promtool unavailable — Docker daemon off, same caveat as the original Chunk 7.2 run).
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

`docs/known-issues.md` was rewritten 2026-09-25 and now matches reality — it holds the per-issue
detail (O-1 … O-3) plus a one-line index of the 8 formerly-listed issues (all verified fixed).
Summary:

1. ~~**O-1 (High): Kafka consumer loop has no reconnect**~~ — **fixed 2026-09-25** (see §4).
2. **O-2 (Low): outbox relay not circuit-breaker-guarded** — nice-to-have; wrap
   `message_broker.send_message` with a `'kafka'` breaker to skip doomed sends during outages.
3. ~~**O-3 (Low): `ruff` missing from the poetry env**~~ — **fixed 2026-09-25** (see §4).
   Lint now: `cd app && poetry run ruff check <changed files>` (v0.15.22, matches pre-commit).

---

## 6. How to update this file

After any meaningful change, edit **section 4** (one bullet: date, one-line what, files touched)
and **section 5** (add/remove issues), and refresh the "Last updated" + test-count line at the top.
Keep the file under ~150 lines — it is a snapshot, not a changelog. Full history lives in git.

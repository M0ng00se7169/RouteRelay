# Project Memory (Agent State)

> **Purpose:** single living snapshot of the project's architecture, recent changes, and open
> issues — so agents can skip re-scanning the repo each session. **Update this file after every
> meaningful change.** Where this file and older docs disagree, this file is newer — but re-verify
> line numbers before editing (files move).
>
> Last updated: **2026-09-25** · Tests: **211 passed** (`cd app && poetry run pytest`, ~3s)

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
- A second, **private** `'kafka'` breaker (same config knobs, NOT registered under the
  `CircuitBreaker` type — that key is the mongo one) guards the outbox relay's sends: open-state
  pre-check skips the batch, sends go through `breaker.call()`, `CircuitOpenError` mid-batch stops
  the batch without a histogram sample. `OutboxRelay.circuit_breaker=None` disables it (tests).
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
- **After code changes:** `cd app && poetry run pytest` (all 211 must stay green).

---

## 4. Recently completed (newest first)

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

---

## 6. How to update this file

After any meaningful change, edit **section 4** (one bullet: date, one-line what, files touched)
and **section 5** (add/remove issues), and refresh the "Last updated" + test-count line at the top.
Keep the file under ~150 lines — it is a snapshot, not a changelog. Full history lives in git.

# Project Memory (Agent State)

> **Purpose:** single living snapshot of the project's architecture, recent changes, and open
> issues — so agents can skip re-scanning the repo each session. **Update this file after every
> meaningful change.** Where this file and older docs disagree, this file is newer — but re-verify
> line numbers before editing (files move).
>
> Last updated: **2026-09-25** · Tests: **217 passed** (`cd app && poetry run pytest`, ~5s) · CI: GitHub Actions (`.github/workflows/ci.yml`: pre-commit lint, pytest, promtool/amtool, docker build)

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
  `APP_IMAGE=ghcr.io/m0ng00se7169/ddd_examples:<sha>` in `.env` (never git-revert to roll back —
  that triggers a fresh CD build). New `deploy/compose/docker-compose.deploy.yml` REPLACES
  `main-app` (compose `-f` override merge cannot remove keys — an `image:` override over
  `app.yaml` would still hit its `build:` and repo bind-mount + `--reload`; verified via
  `docker compose config`: no build/volumes in the rendered merge, APP_IMAGE resolves).
  Blast-radius table: Mongo/Kafka/AM state survives an app swap; the app is stateless by design
  (outbox). `.github/workflows/cd.yml`: on push to `main` → wait for
  the CI run's `Tests (pytest)` check on the same SHA (`lewagon/wait-on-check-action`) → buildx
  build + push to `ghcr.io/m0ng00se7169/ddd_examples` (tags: `latest` on the default branch +
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

---

## 6. How to update this file

After any meaningful change, edit **section 4** (one bullet: date, one-line what, files touched)
and **section 5** (add/remove issues), and refresh the "Last updated" + test-count line at the top.
Keep the file under ~150 lines — it is a snapshot, not a changelog. Full history lives in git.

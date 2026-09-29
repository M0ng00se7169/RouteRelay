# Known Issues

Concrete bugs and tech-debt in the codebase, **rewritten and verified against the source on
2026-09-25**. For the living project snapshot (architecture, recent changes) see
`PROJECT_MEMORY.md`; this file holds the per-issue detail. Re-check file:line references before
editing — code moves.

---

## Open issues

None — all filed issues are fixed (see the index below). This space is intentionally kept for
future findings.

---

## Fixed issues (formerly #1–#8, verified fixed as of 2026-09-25)

Kept as a one-line index so old references don't dangle; details live in git history.

| # | Was | Status |
|---|-----|--------|
| 1 | `ListenerAddedEventHandler.handle` wrong event type annotation | Fixed — handler refactored (`app/logic/events/messages.py`) |
| 2 | `send_message` argument order mismatch (base vs Kafka) | Fixed — both are `(self, topic, key, value)` now |
| 3 | `BaseConnectionManager` registered twice in `init.py` | Fixed — single registration (now a `create_connection_manager()` factory, `init.py:259`) |
| 4 | `TelegramNotificationClient` never registered | Fixed — registered in `lifespan.py`, wired when `telegram_bot_token` is set |
| 5 | Inbound Kafka→WebSocket relay never started | Fixed — `_kafka_consumer_loop` in `lifespan.py` (but see O-1) |
| 6 | `MemoryChatRepository` implements only 3/7 methods | Fixed — all 7 implemented (+ extras) |
| 7 | `init_dummy_container` doesn't override `BaseMessagesRepository` | Fixed — both repos overridden (`app/test/fixtures.py`) |
| 8 | Command handlers not uniformly top-level registered | Fixed — all handlers factory-registered in `_init_container` |
| O-1 | Kafka consumer loop: no reconnect/backoff; `kafka_consumer_up` missed clean loop exits | Fixed 2026-09-25 — reconnect loop with exponential backoff (`KAFKA_CONSUMER_BACKOFF_*`), `kafka_consumer_reconnects_total{topic}` metric, heartbeat drops on any non-cancelled task completion |
| O-3 | `ruff` missing from the poetry env — `poetry run ruff check` failed despite the pre-commit gate | Fixed 2026-09-25 — `ruff@^0.15.22` added to dev deps (pinned to match the pre-commit hook version), lock file updated; the documented verify command works |
| O-2 | Outbox relay not circuit-breaker-guarded — one doomed send + producer timeout per tick during a Kafka outage | Fixed 2026-09-25 — optional `'kafka'` breaker (private instance in `create_outbox_relay`, same config knobs as `'mongo'`): open-state pre-check skips the batch (rows stay unsent), sends wrapped in `breaker.call()`, `CircuitOpenError` mid-batch skips the histogram sample; `circuit_breaker=None` keeps the legacy path |

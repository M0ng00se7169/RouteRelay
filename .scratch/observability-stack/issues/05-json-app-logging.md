# 05 — JSON-structured app logging

**What to build:** The FastAPI app emits JSON-structured log lines (timestamp, level, logger,
message) instead of plain text, so Loki receives clean, queryable labels. This is an additive change
wired once at app startup; it must not affect the existing test suite.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A small logging-config module builds a stdlib JSON formatter and attaches it to the root logger.
- [ ] `create_app()` invokes it once during startup, before the Prometheus instrumentator.
- [ ] The change has no infrastructure imports (respects domain/logic layering).
- [ ] `cd app && poetry run pytest` remains green.
- [ ] Running the app produces JSON log lines that Loki/Promtail can parse into `level`/`logger`.

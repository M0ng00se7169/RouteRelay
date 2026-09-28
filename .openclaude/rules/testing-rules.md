---
description: Rules for writing isolated tests against in-memory repos; run pytest from app/; no Motor/network imports in tests
globs: app/test/**/*.py
alwaysApply: false
---

# Testing Rules

Tests must run with **zero external infrastructure** (no Mongo, no Kafka) using in-memory repos. This
enforces `CLAUDE.md` ("Tests use in-memory repos") as a concrete rule. Operational setup is in
`docs/local-development.md`.

## Rules

1. **Run pytest from inside `app/`.** The flat first-party imports (`application`, `domain`,
   `infrastructure`, `logic`, `settings`, `test`) only resolve when `app/` is on `sys.path`.
   ```bash
   cd app && uv run pytest
   ```

2. **Use the in-memory container.** Test API clients must apply the swap from
   `app/test/fixtures.py` (`init_dummy_container`) via FastAPI `dependency_overrides`, exactly as
   `app/test/application/api/conftest.py:13` does:
   ```python
   app.dependency_overrides[init_container] = init_dummy_container
   ```
   `init_dummy_container` rebinds `BaseChatsRepository` → `MemoryChatRepository`.

3. **Message tests need extra setup.** `init_dummy_container` does **not** override
   `BaseMessagesRepository` (known issue #7). For message endpoints, register a memory messages repo
   in your fixture or the test will hit real Mongo and fail/timeout.

4. **No Motor / network imports in tests.** A test that imports `motor`, opens a socket, or relies on
   a live broker is not isolated. Use the memory repo + the mediator.

5. **Keep domain/logic free of infra imports** — including in test code. Tests may import from
   `application`, `logic`, `domain`, `test`, but never instantiate `AsyncIOMotorClient` or
   `KafkaMessageBroker`.

6. **Cover the happy path and the documented edge cases.** If you touch a handler flagged in
   `docs/known-issues.md`, add a regression test for the fix.

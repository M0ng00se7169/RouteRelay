---
description: Run the project test suite from app/ (correct sys.path) and summarize failures.
---

# /run-tests

Run the test suite for this FastAPI + Kafka chat app. Tests use in-memory repositories, so no Mongo or
Kafka is needed — but pytest **must** run with `app/` on the path so the flat first-party imports
(`application`, `domain`, `infrastructure`, `logic`, `settings`, `test`) resolve.

## Command
```bash
cd app && uv run pytest
```
If uv isn't available in the shell, fall back to the project `.venv`: `.venv/Scripts/activate` (Windows) or `.venv/bin/activate`, then `cd app && pytest`.

## What to do
1. Run the command above.
2. If the user passed a path or `-k` filter, append it (e.g. `cd app && uv run pytest -k messages`).
3. Summarize the result: count of passed/failed/skipped, and for each failure the file and the
   assertion/error.
4. If failures are import errors (`ModuleNotFoundError` for `application`/`logic`/...), the cause is
   almost always that the command did not `cd app` first — re-run from `app/`.
5. If a message-path test hits real Mongo (timeout/connection error), note known issue #7
   (`init_dummy_container` does not override `BaseMessagesRepository`).

## Rules
- Never run pytest from the repo root — it will fail on imports.
- Do not modify tests to make them pass; report failures honestly.

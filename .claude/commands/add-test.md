---
description: Scaffold a new in-memory test for an endpoint or handler, using init_dummy_container + FastAPI dependency_overrides.
---

# /add-test

Scaffold an isolated test for this FastAPI + Kafka chat app. Tests must run with no Mongo/Kafka, using
in-memory repositories per `.claude/rules/testing-rules.md`.

## Inputs
Ask the user (or take from the prompt) for:
1. **Target**: the endpoint (e.g. `POST /chat`, `POST /chat/{id}/messages`) or handler to cover.
2. **Scenario**: happy path, not-found, duplicate, etc.

## Process (read first, then generate the test)
1. Read `app/test/fixtures.py` (`init_dummy_container`) and `app/test/application/api/conftest.py`
   (the `app`/`client` fixtures that apply `app.dependency_overrides[init_container] = init_dummy_container`).
2. Read an existing test under `app/test/` that targets a similar endpoint to mirror the style.
3. Generate a test module that:
   - Uses the existing `client` fixture (so the in-memory container swap is active).
   - For **message** endpoints, also overrides `BaseMessagesRepository` with a memory variant
     (known issue #7 — `init_dummy_container` only overrides `BaseChatsRepository`).
   - Asserts status code + response shape; does **not** import `motor`, open sockets, or require a
     live broker.
4. Place it next to the related tests under `app/test/...`.

## Reminder for the user
Run it with: `cd app && poetry run pytest <path>`. Tests only resolve imports when run from `app/`.

Do not edit files unless the user asks — just produce the test code.

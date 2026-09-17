---
description: Workflow for scaffolding a new CQRS feature (command/query/event → handler → DI → API route → test)
globs: app/**/*.py
alwaysApply: false
---

# Feature Scaffold Workflow

Use this checklist whenever a new capability is added to the app. It complements `CLAUDE.md`
("Before modifying") with the exact steps and files. Copy the existing handlers in
`app/logic/commands/messages.py` and `app/logic/events/messages.py` as the reference shape.

## Steps

1. **Classify the change.**
   - Write path → a **Command** (`<X>Command` / `<X>CommandHandler`).
   - Read path → a **Query** (`<X>Query` / `<X>QueryHandler`).
   - Side effect on state change → a domain **Event** in `app/domain/events/messages.py`, handled by
     an `<X>EventHandler` in `app/logic/events/messages.py`.

2. **Write the frozen dataclass + handler** in the matching `app/logic/{commands,queries,events}/`
   file. Follow `docs/cqrs-contract.md`:
   - Command handler: first field `_mediator: EventMediator`, then deps; end with
     `await self._mediator.publish(entity.pull_events())`.
   - Event handler: inject `message_broker`, `connection_manager`, `broker_topic`; `handle` is
     `async`.

3. **Register the handler in DI** (`app/logic/init.py`):
   - Add a top-level `container.register(<X>Handler)` (see `docs/di-reference.md`). Do this for
     **every** handler — including commands — so `container.resolve` works (known issue #8).
   - Wire it into `init_mediator()` with `mediator.register_{command,query,event}(...)`.
   - Do **not** double-register `BaseConnectionManager` (known issue #3).

4. **Add the API route + schema** under `app/application/api/` (`messages/`):
   - Route delegates to `mediator.handle_command(...)` / `handle_query(...)`.
   - Response schema uses a `from_entity` classmethod (see existing schemas in
     `app/application/api/messages/schemas.py`).

5. **Add a test** with the in-memory container — see `.claude/rules/testing-rules.md`.

6. **Before touching event handlers**, check `docs/known-issues.md` (#1, #2, #4, #5) so you don't
   copy a latent bug.

## Definition of done
- Handler present, frozen dataclass where required, `_mediator`/`publish` convention followed.
- `init.py` top-level registration + `init_mediator` registration both present.
- Route + `from_entity` schema present.
- At least one in-memory test covering the happy path.
- `poetry run pre-commit run --all-files` passes (Ruff line-length 100, single quotes, tabs).

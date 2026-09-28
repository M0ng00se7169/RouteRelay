---
description: Scaffold a new CQRS feature end-to-end — emits the checklist of files to touch (command/query/event → handler → DI → API route → schema → test).
---

# /add-feature

Scaffold a new capability for this FastAPI + Kafka chat app by following
`.claude/rules/feature-workflow.md`.

## Inputs
Ask the user (or take from the prompt) for:
1. **Feature name** (e.g. `RenameChat`, `MuteListener`) — PascalCase noun.
2. **Kind**: `command` (write), `query` (read), or `event` (side effect). A feature can be a mix.
3. One-line **description** of what it does.

## Process
1. Read the reference docs: `.claude/rules/feature-workflow.md`, `docs/cqrs-contract.md`,
   `docs/di-reference.md`, and `docs/known-issues.md` (to self-flag risks).
2. Inspect the existing handlers in `app/logic/{commands,queries,events}/messages.py` and routes in
   `app/application/api/messages/` to mirror the exact shape.
3. Emit an actionable to-do checklist:

```
Feature: <name> (<kind>) — <description>

[ ] 1. Domain model: if a new domain event, add `<X>Event` to app/domain/events/messages.py
       (frozen dataclass, event_title: ClassVar[str], inherit BaseEvent).
[ ] 2. Logic: add `<X>Command`/`CommandHandler` (or Query/Event) to app/logic/<kind>s/messages.py
       - frozen @dataclass; CommandHandler first field `_mediator: EventMediator`
       - end command handler with `await self._mediator.publish(entity.pull_events())`
       - event handler: inject message_broker, connection_manager, broker_topic; `async def handle`
[ ] 3. DI: add top-level `container.register(<X>Handler)` in app/logic/init.py (near :102)
       AND wire it in init_mediator() with mediator.register_<kind>(...).
       (Do NOT double-register BaseConnectionManager — issue #3.)
[ ] 4. API: add route + Pydantic response schema (with `from_entity`) under
       app/application/api/messages/. Route calls mediator.handle_command/handle_query.
[ ] 5. Test: add an in-memory test (app/test/...) using init_dummy_container + dependency_overrides.
       For message paths, also override BaseMessagesRepository (issue #7). Run: cd app && pytest
[ ] 6. Lint: uv run pre-commit run --all-files
```

## Known issues to watch (cite docs/known-issues.md)
- Event handlers: wrong annotation (#1), `send_message` arg order (#2).
- Telegram pipeline unwired (#4); inbound Kafka consumer dead (#5).
- Memory repo gap (#6); messages not overridden in tests (#7).

Do not edit files unless the user asks — just produce the checklist.

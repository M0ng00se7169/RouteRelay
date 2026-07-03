---
description: Enforce that all handlers/repos/brokers are registered in app/logic/init.py; singleton vs scoped guidance
globs: app/**/*.py
alwaysApply: false
---

# DI Registration Rule

`app/logic/init.py` is the **single DI source of truth** (per `CLAUDE.md` → "Core patterns"). Every
new repository, message broker, connection manager, and handler must be wired there. Reference table:
`docs/di-reference.md`.

## Rules

1. **No wiring outside `init.py`.** Do not instantiate infrastructure or handlers ad-hoc in routers
   or logic. Resolve them from the container.

2. **Always register every handler top-level.** Add `container.register(<X>Handler, scope=...)` next
   to the other handler registrations (currently `init.py:102`–`109`). This makes both
   `container.resolve(<X>Handler)` and `init_mediator` work. Skipping it for command handlers is a
   known trap (known issue #8).

3. **Wire handlers into the mediator** inside `init_mediator()` with
   `mediator.register_{command,query,event}(...)`. The mediator is built last, after all handler
   registrations.

4. **Scope choice:**
   - `Scope.singleton` → long-lived infrastructure: `Config`, `AsyncIOMotorClient`,
     `BaseMessageBroker`, `BaseConnectionManager`, the `MongoDBChats/MessagesRepository`.
   - default scope → handlers (rebuilt per resolve; cheap).

5. **Do not double-register.** `BaseConnectionManager` is already registered twice (`init.py:122`
   and `:227`, known issue #3). Do not add a third.

6. **Don't re-pin an existing registration** when adding a new handler — only add your new lines.

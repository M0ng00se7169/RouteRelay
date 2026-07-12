---
description: Enforce that new repository methods are implemented in both mongo.py and memory.py behind the ABC
globs: app/infrastructure/repositories/**/*.py
alwaysApply: false
---

# Repository Contract Rule

Repositories live under `app/infrastructure/repositories/messages/` in three flavors:
`base.py` (ABC — the contract), `mongo.py` (Motor, production), `memory.py` (in-memory, tests). This
is mandated by `CLAUDE.md` ("Repository split") and the "Before modifying" checklist.

## Rules

1. **`base.py` is the contract.** Any method added to a repository must be declared `@abstractmethod`
   on the ABC (`BaseChatsRepository` / `BaseMessagesRepository`) first.

2. **Implement in BOTH `mongo.py` and `memory.py`.** A method on the ABC with only one
   implementation will raise `NotImplementedError` for the other backend. The memory repo currently
   implements only 3 of 7 `BaseChatsRepository` methods (known issue #6) — do not widen that gap.

3. **Tests depend on the memory repo.** `app/test/fixtures.py` (`init_dummy_container`) binds
   `BaseChatsRepository` → `MemoryChatRepository`; it does **not** yet override
   `BaseMessagesRepository` (known issue #7). If you add a messages method, you must add a memory
   variant and, for message tests, register the memory messages repo.

4. **Never import Motor in `domain/` or `logic/`.** Repositories (infrastructure) hold all Mongo
   access. Handlers receive the ABC type, not a concrete Motor-backed impl.

5. **Mirror behavior, not implementation.** The memory repo should reproduce the observable behavior
   of the Mongo one (queries, filters, existence checks) so tests are meaningful.

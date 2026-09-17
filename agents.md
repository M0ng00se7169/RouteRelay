# Agents Guide

This file contains guidance specific to how agents should interact with the codebase.
It complements `.claude/rules/*.md` (structural rules) and `CLAUDE.md` (architectural overview).

---

## Project Context

**FastAPI + Kafka + MongoDB** multi-user chat backend demonstrating DDD, CQRS, and event-driven architecture.

| Aspect | Location |
|--------|----------|
| DI container | `app/logic/init.py` — **single source of truth** |
| Mediator | `app/logic/mediator/base.py` |
| Entities | `app/domain/entities/messages.py` |
| Test fixtures | `app/test/fixtures.py` (`init_dummy_container`) |

**Flat import aliases** (`application`, `domain`, `infrastructure`, `logic`, `settings`, `tests`) — see `docs/cqrs-contract.md`.

---

## Tool Use Rules

1. **Always use `read` before editing** — you have the file in context already, it won't be in the agent's context.
2. **Edit instead of shell** — `edit_file` for modifications, `execute_command` only for non-edit tasks.
3. **Non-interactive CLI** — use flags that avoid prompts (`--yes`, `--force`, `--no-audit`, etc.).
4. **Test after code changes** — `cd app && poetry run pytest` or `poetry run uvicorn --factory application.api.main:create_app --reload`.

---

## Agent Workflow

1. **THINK**: Analyze the request, identify relevant files, check current state via tools, anticipate edge cases.
2. **PLAN**: Formulate a concise plan. Use `plan mode` to enter planning mode.
3. **ACT**: Execute the plan using tools. Use `task create` to track multi-step work.
4. **VERIFY**: Run tests/linters after code changes. Use `verify` command.

---

## Import Patterns

- **Do**: `from application.api.user_routes import router`
- **Do**: `from infrastructure.repositories.messages.memory import MemoryChatRepository`
- **Don't**: `from domain.entities.chat import Chat` — domain imports must be `domain` (flat alias)

---

## Known Issues to Watch

See `docs/known-issues.md`. Common gotchas:
- `BaseConnectionManager` registered twice (init.py:122, :227)
- `init_dummy_container` does NOT override `BaseMessagesRepository` — message tests need extra setup
- `MemoryChatRepository` only implements 3/7 `BaseChatsRepository` methods

---

## Questions

- **How is the app wired?** → `read app/logic/init.py`
- **How do I add a new handler?** → Follow `docs/cqrs-contract.md`, register in `init.py`, add test
- **What's the test command?** → `cd app && poetry run pytest`

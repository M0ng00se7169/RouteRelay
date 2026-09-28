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

## Project Memory (READ FIRST)

**`PROJECT_MEMORY.md`** is the living snapshot of the project: current architecture, recently
completed changes, and verified open issues. **Read it at session start** — it saves scanning the
repo — and **update it (sections 4–5) after every meaningful change**. It is newer than the docs;
if it and an older doc disagree, trust it but re-verify line numbers.

---

## Tool Use Rules

1. **Always use `read` before editing** — you have the file in context already, it won't be in the agent's context.
2. **Edit instead of shell** — `edit_file` for modifications, `execute_command` only for non-edit tasks.
3. **Non-interactive CLI** — use flags that avoid prompts (`--yes`, `--force`, `--no-audit`, etc.).
4. **Test after code changes** — `cd app && uv run pytest` or `uv run uvicorn --factory application.api.main:create_app --reload`.

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

`docs/known-issues.md` (rewritten 2026-09-25) holds the verified open issues with detail;
`PROJECT_MEMORY.md` §5 summarizes. Top items:
- ~~Kafka consumer reconnect~~ **fixed 2026-09-25** (O-1): reconnect/backoff + reconnect metric;
  `kafka_consumer_up` stays 1 through reconnects, drops on stop/any task exit
- ~~Outbox relay not circuit-breaker-guarded~~ **fixed 2026-09-25** (O-2): the relay now runs its
  sends through a private `'kafka'` breaker (`create_outbox_relay`); open breaker skips the batch
- ~~`ruff` missing from the poetry env~~ **fixed 2026-09-25** (O-3): `uv run ruff check <files>`
  works now — run it on changed files as part of verification

---

## Questions

- **How is the app wired?** → `read app/logic/init.py`
- **How do I add a new handler?** → Follow `docs/cqrs-contract.md`, register in `init.py`, add test
- **What's the test command?** → `cd app && uv run pytest`

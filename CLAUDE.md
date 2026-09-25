# CLAUDE.md

FastAPI + Kafka + MongoDB chat backend — DDD, CQRS, event-driven.

## Quick references

| Topic | Location |
|-------|----------|
| **Living project state (read first)** | `PROJECT_MEMORY.md` |
| Architecture overview | `docs/architecture.md` |
| Local dev & Makefile | `docs/local-development.md` |
| Code conventions & patterns | `.claude/rules/*.md` |
| Known bugs & latent issues | `docs/known-issues.md` |
| Agent behavior & tool rules | `.claude/agents.md` |
| DI wiring reference | `docs/di-reference.md` |

## Request flow (create message)

```
POST /chat/{id}/messages
  ↓
application/api/messages/handlers.py → mediator.handle_command
  ↓
handler validates chat, builds Message, saves
  ↓
mediator.publish(chat.pull_events())
  ↓
NewMessageReceivedEventHandler (Kafka + WebSocket)
  ↓
response
```

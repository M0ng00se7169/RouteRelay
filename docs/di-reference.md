# DI Reference

Authoritative map of every registration in the `punq` container wired by `app/logic/init.py`.
This is the single DI source of truth (see `CLAUDE.md` → "Core patterns" → "Dependency injection").
This file is a *table of what is registered*, not a re-explanation of the pattern.

## How wiring works

- `init_container()` (`app/logic/init.py:66`) is `@lru_cache(1)` — one container per process.
- `Mediator` is built last, inside `init_mediator()` (`:124`), by manually instantiating each
  handler with its resolved dependencies and calling `mediator.register_{command,query,event}`.
- Handlers are resolved from the container at registration time, so a handler must be registered
  *before* `init_mediator` runs (top-level `container.register` calls at `:102`–`:109`, `:225`).
- Singleton vs default scope: infrastructure pieces are `Scope.singleton`; handlers are left at the
  default scope (a fresh instance is built per `container.resolve`).

## Registration table

| Register call (file:line) | Registered type | Implementation | Scope |
|---|---|---|---|
| `init.py:74` | `Config` | `Config()` instance | singleton |
| `init.py:81` | `AsyncIOMotorClient` | `create_mongodb_client()` factory | singleton |
| `init.py:98` | `BaseChatsRepository` | `MongoDBChatsRepository` (factory) | singleton |
| `init.py:99` | `BaseMessagesRepository` | `MongoDBMessagesRepository` (factory) | singleton |
| `init.py:102` | `CreateChatCommandHandler` | class | default |
| `init.py:103` | `CreateMessageCommandHandler` | class | default |
| `init.py:106` | `GetChatDetailQueryHandler` | class | default |
| `init.py:107` | `GetMessagesQueryHandler` | class | default |
| `init.py:108` | `GetAllChatsQueryHandler` | class | default |
| `init.py:109` | `GetAllChatsListenersQueryHandler` | class | default |
| `init.py:121` | `BaseMessageBroker` | `KafkaMessageBroker` (factory) | singleton |
| `init.py:122` | `BaseConnectionManager` | `ConnectionManager()` instance | singleton |
| `init.py:225` | `Mediator` | `init_mediator()` factory | default |
| `init.py:226` | `EventMediator` | `init_mediator()` factory | default |
| `init.py:227` | `BaseConnectionManager` | `ConnectionManager()` instance | singleton |

> **Note on query handlers:** query handlers are registered top-level (`:106`–`:109`) *and* resolved
> again inside `init_mediator` (`:200`, `:204`, `:208`, `:212`) for `register_query`. The top-level
> register is what makes `container.resolve(GetChatDetailQueryHandler)` work in tests — keep it.

## Mediator event/command/query registrations

Inside `init_mediator` (`init.py:124`–`223`):

- Events: `NewChatCreatedEvent`, `NewMessageReceivedEvent`, `NewMessageReceivedFromBrokerEvent`,
  `ChatDeletedEvent`, `ListenerAddedEvent`.
- Commands: `CreateChatCommand`, `CreateMessageCommand`, `DeleteChatCommand`,
  `AddTelegramListenerCommand`.
- Queries: `GetChatDetailQuery`, `GetAllChatsListenersQuery`, `GetMessagesQuery`, `GetAllChatsQuery`.

> **Asymmetry worth knowing:** `DeleteChatCommandHandler` and `AddTelegramListenerCommandHandler`
> are **not** top-level registered (no `container.register` line) — they are only manually
> instantiated inside `init_mediator`. If you add code that does `container.resolve(DeleteChatCommandHandler)`,
> it will fail. Prefer adding a top-level `container.register(...)` for new handlers so both paths work.

## Two known wiring issues (tracked in `docs/known-issues.md`)

1. `BaseConnectionManager` is registered **twice** — at `init.py:122` and `init.py:227`. Harmless
   (identical instance), but redundant. Reconcile by removing one.
2. `TelegramNotificationClient` (`app/infrastructure/integrations/notifications/clients/telegram.py`)
   exists but is **never registered** in `init.py`; no handler emits to Telegram. See known-issues #4.

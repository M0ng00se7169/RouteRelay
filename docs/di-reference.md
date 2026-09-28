# DI Reference

Authoritative map of every registration in the `punq` container wired by `app/logic/init.py`.
This is the single DI source of truth (see `CLAUDE.md` → "Core patterns" → "Dependency injection").
This file is a *table of what is registered*, not a re-explanation of the pattern.

## How wiring works

- `init_container()` (`app/logic/init.py:97`) is `@lru_cache(1)` — one container per process.
- `Mediator` is built last, by `build_mediator()` (`:313`), which manually instantiates each
  handler with its resolved dependencies and calls `mediator.register_{command,query,event}`.
- Handlers are resolved from the container at registration time, so a handler must be registered
  *before* `build_mediator` runs (top-level `container.register` calls at `:236`–`:240`).
- Singleton vs default scope: infrastructure pieces are `Scope.singleton`; handlers are left at the
  default scope (a fresh instance is built per `container.resolve`).

## Registration table

| Register call (file:line) | Registered type | Implementation | Scope |
|---|---|---|---|
| `init.py:115` | `Config` | `Config()` instance | singleton |
| `init.py:122` | `AsyncIOMotorClient` | `create_mongodb_client()` factory | singleton |
| `init.py:138` | `CircuitBreaker` | `create_mongo_circuit_breaker()` (name `'mongo'`, wraps both repo factories below) | singleton |
| `init.py:163` | `BaseCacheClient` | `create_cache_client()` — `ValkeyCacheClient` (with a private `'valkey'` breaker) when any ADR-0008 flag is on, else `MemoryCacheClient` | singleton |
| `init.py:171` | `BasePresenceTracker` | `create_presence_tracker()` → `ValkeyPresenceTracker` over the cache client | singleton |
| `init.py:184` | `BaseDistributedLock` | `create_relay_lease()` → `ValkeyLeaseLock`, or **`None`** when `RELAY_LOCK_ENABLED=false` | singleton |
| `init.py:223` | `BaseChatsRepository` | `CircuitBreakerChatsRepository(  [CachedChatsRepository(  MongoDBChatsRepository)])` (factory) | singleton |
| `init.py:224` | `BaseMessagesRepository` | `CircuitBreakerMessagesRepository(  [CachedMessagesRepository(  MongoDBMessagesRepository)])` (factory) | singleton |
| `init.py:232` | `BaseOutboxRepository` | `init_outbox_mongodb_repository()` factory | singleton |
| `init.py:233` | `SessionProvider` | `MongoSessionProvider(client=client)` instance | singleton |
| `init.py:236` | `GetChatDetailQueryHandler` | class | default |
| `init.py:237` | `GetMessagesQueryHandler` | class | default |
| `init.py:238` | `GetAllChatsQueryHandler` | class | default |
| `init.py:239` | `GetAllChatsListenersQueryHandler` | class | default |
| `init.py:240` | `GetChatPresenceQueryHandler` | class | default |
| `init.py:249` | `BaseMessageBroker` | `create_message_broker()` → `KafkaMessageBroker` | singleton |
| `init.py:259` | `BaseConnectionManager` | `create_connection_manager()` → `ConnectionManager(presence_tracker=… or None)` | singleton |
| `init.py:281` | `OutboxRelay` | `create_outbox_relay()` factory (private `'kafka'` breaker + optional lease) | singleton |
| `init.py:307` | `Mediator` | `build_mediator()` result | singleton |
| `init.py:308` | `EventMediator` | the same `build_mediator()` result | singleton |
| `init.py:362`–`:365` | `CreateChatCommandHandler`, `CreateMessageCommandHandler`, `DeleteChatCommandHandler`, `AddTelegramListenerCommandHandler` | factories resolving their deps from this container | default |
| `init.py:295` | `BaseNotificationClient` | `TelegramNotificationClient` — **only** when `telegram_bot_token` is set | singleton |

> **Circuit breaker:** the `Base{Chats,Messages}Repository` implementations are wrapped in breaker
> proxies (`app/infrastructure/resilience.py`); when the shared `'mongo'` breaker is open, calls fail
> fast with `CircuitOpenError`, mapped to **HTTP 503 + Retry-After** by the app-level handler in
> `application/api/main.py`. Test containers override these registrations with in-memory repos and
> never see the proxies (`init_dummy_container(wrap_repos_with_breaker=True)` opts in for the 503 test).
> The outbox relay (`OutboxRelay` singleton, built by `create_outbox_relay`) gets its own **private
> `'kafka'` breaker** — same config knobs, deliberately not registered under the `CircuitBreaker`
> type (that key is the mongo one). While open, the relay skips the outbox batch (rows stay
> unsent); `circuit_breaker=None` disables the guard. The cache client carries a third **private
> `'valkey'` breaker** the same way, and swallows `CircuitOpenError` internally so a cache outage
> never becomes a 503.

> **ADR-0008 wiring (cache/presence/lock):** the cache proxies are wired **inside** the breaker
> proxies, i.e. `CircuitBreaker(Cached(Mongo))`. That order is deliberate: the `'mongo'` breaker
> must keep measuring Mongo only, so a Valkey error has to be swallowed below it. Every
> `BaseCacheClient`-consuming collaborator (the command handlers, the presence tracker, the relay
> lease) resolves the same singleton, so none of them branch on `None` — with all three flags off it
> is a `MemoryCacheClient` and the features are simply inert.
> `init_dummy_container` overrides it with the in-memory client and can opt into the production
> proxy order via `wrap_repos_with_cache=True` / `presence_enabled=True`.

> **Note on query handlers:** query handlers are registered top-level (`:236`–`:240`) *and* resolved
> again inside `build_mediator` for `register_query`. The top-level register is what makes
> `container.resolve(GetChatDetailQueryHandler)` work in tests — keep it.

## Mediator event/command/query registrations

Inside `build_mediator` (`init.py:313`):

- Events: `NewChatCreatedEvent`, `NewMessageReceivedEvent`, `NewMessageReceivedFromBrokerEvent`,
  `ChatDeletedEvent`, `ListenerAddedEvent`.
- Commands: `CreateChatCommand`, `CreateMessageCommand`, `DeleteChatCommand`,
  `AddTelegramListenerCommand`.
- Queries: `GetChatDetailQuery`, `GetAllChatsListenersQuery`, `GetMessagesQuery`,
  `GetAllChatsQuery`, `GetChatPresenceQuery`.

> **Asymmetry worth knowing:** `DeleteChatCommandHandler` and `AddTelegramListenerCommandHandler`
> are **not** top-level registered (no `container.register` line) — they are only manually
> instantiated inside `build_mediator`. If you add code that does `container.resolve(DeleteChatCommandHandler)`,
> it will fail. Prefer adding a top-level `container.register(...)` for new handlers so both paths work.

## Known wiring issues (tracked in `docs/known-issues.md`)

1. `TelegramNotificationClient` exists but is registered only as `BaseNotificationClient` (and only
   when a bot token is configured); `ListenerAddedEventHandler` resolves the ABC, not the concrete
   class. See known-issues #4.

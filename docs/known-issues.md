# Known Issues

Concrete bugs and tech-debt found in the codebase but **not** covered by `CLAUDE.md` or `README.md`.
Each item lists the exact file:line, the impact, and a suggested fix. Severity is a rough guide.

> Line references were verified against the source at time of writing. If a file has changed, re-check
> before acting.

## #1 — `ListenerAddedEventHandler.handle` wrong event type annotation
**File:** `app/logic/events/messages.py:31`
**Severity:** Medium (latent bug)

The class is bound to `EventHandler[ListenerAddedEvent, None]` (`:30`), but `handle` is annotated
`event: NewChatCreatedEvent` (`:31`). Because `publish` dispatches by `event.__class__` and the
handler body uses `event.event_id` (present on the base `BaseEvent`), it *works at runtime today*, but
the type is wrong and will mislead readers/type-checkers.

**Fix:** change `:31` to `async def handle(self, event: ListenerAddedEvent) -> None:`.

## #2 — `send_message` argument order mismatch (base vs Kafka)
**File:** `app/infrastructure/message_brokers/base.py:20` vs `kafka.py:15`
**Severity:** Low (benign)

ABC signature is `send_message(self, topic: str, key: bytes, value: bytes)`; the Kafka implementation
is `send_message(self, key: bytes, topic: str, value: bytes)`. All callers in
`app/logic/events/messages.py` use **keyword arguments**, so no runtime break. Fragile if anyone ever
calls positionally.

**Fix:** align the Kafka implementation to `(self, topic, key, value)`.

## #3 — `BaseConnectionManager` registered twice
**File:** `app/logic/init.py:122` and `:227`
**Severity:** Low

Same `ConnectionManager()` singleton registered at both lines. Harmless (identical instance), but
redundant and a copy-paste trap. See also `docs/di-reference.md`.

**Fix:** delete one registration (prefer keeping `:122`, which is before `init_mediator` runs).

## #4 — `TelegramNotificationClient` exists but is not wired
**Files:** `app/infrastructure/integrations/notifications/clients/telegram.py`; not referenced in `app/logic/init.py`
**Severity:** Medium (dead integration)

`AddTelegramListenerCommand`/`Handler` and the `ListenerAddedEvent` pipeline exist, but the Telegram
client is never registered in the container and no event handler sends to Telegram. The "telegram
listener" feature half-wires: it stores the listener id in Mongo but never notifies Telegram.

**Fix (when implementing):** register `TelegramNotificationClient` in `init.py`, add a
`TelegramNotificationEventHandler` (or extend `ListenerAddedEventHandler`) that publishes via the
client, and register it for `ListenerAddedEvent`. Track under an issue until done.

## #5 — Inbound Kafka→WebSocket relay is never started
**Files:** `app/infrastructure/message_brokers/kafka.py:26` (`start_consuming`); `app/application/api/lifespan.py`
**Severity:** Medium (dead code path)

`KafkaMessageBroker.start_consuming` exists and `NewMessageReceivedFromBrokerEvent` /
`...Handler` are registered, but the app lifespan (`lifespan.py`) only `start()`s / `close()`s the
broker — it never starts a consumer loop that pushes broker messages to WebSocket clients. The
inbound path is dead unless an external consumer drives it (e.g. `app/kafka_test_consumer.py`).

**Fix (when implementing):** start the consumer in `lifespan.py` (background task) that yields
messages and publishes `NewMessageReceivedFromBrokerEvent` to the mediator. See `CLAUDE.md` →
"Known gotchas".

## #6 — Memory repo implements only 3 of 7 `BaseChatsRepository` methods
**Files:** `app/infrastructure/repositories/messages/base.py` (7 abstract methods) vs `memory.py:10`–`33` (3 implemented)
**Severity:** Medium (test gap)

`MemoryChatRepository` implements `get_chat_by_oid`, `check_chat_exists_by_title`, `add_chat`. The
other four — `get_all_chats`, `delete_chat_by_oid`, `add_telegram_listener`, `get_all_chat_listeners`
— are missing. Any test that exercises `DeleteChat`, `GetAllChats`, `AddTelegramListener`, or
`GetAllChatsListeners` against the memory repo will raise `NotImplementedError`.

**Fix (when adding a test for those paths):** implement the missing methods in `memory.py` (mirror the
Mongo behavior in `mongo.py`), or document that those handlers are not yet memory-testable.

## #7 — `init_dummy_container` only overrides `BaseChatsRepository`
**Files:** `app/test/fixtures.py:10`–`14`
**Severity:** Medium (test gap)

`init_dummy_container` rebinds `BaseChatsRepository` → `MemoryChatRepository` but **not**
`BaseMessagesRepository`. Tests that post messages / read messages will resolve the Mongo-backed
`MongoDBMessagesRepository` and hit real Mongo unless they override it manually.

**Fix (when adding message tests):** also register the memory messages repository (a
`MemoryMessagesRepository` does not yet exist — create one mirroring `MemoryChatRepository`, or
override `BaseMessagesRepository` in the test's fixture).

## #8 — Command handlers not uniformly top-level registered
**File:** `app/logic/init.py:102`–`109` vs `:136`–`163`
**Severity:** Low

`CreateChatCommandHandler`, `CreateMessageCommandHandler`, and all query handlers are top-level
`container.register`'d. `DeleteChatCommandHandler` and `AddTelegramListenerCommandHandler` are only
manually instantiated inside `init_mediator`. `container.resolve(DeleteChatCommandHandler)` would
fail. See `docs/di-reference.md`.

**Fix (when adding a handler):** always add a top-level `container.register(<X>Handler)` so both the
container and `init_mediator` can build it.

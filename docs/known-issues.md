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

1: # Known Issues
2: 
3: Concrete bugs and tech-debt found in the codebase but **not** covered by `CLAUDE.md` or `README.md`.
4: Each item lists the exact file:line, the impact, and a suggested fix. Severity is a rough guide.
5: 
6: > Line references were verified against the source at time of writing. If a file has changed, re-check
7: > before acting.
8: 
9: ## #1 — `ListenerAddedEventHandler.handle` wrong event type annotation
10: **File:** `app/logic/events/messages.py:31`
11: **Severity:** Medium (latent bug)
12: 
13: The class is bound to `EventHandler[ListenerAddedEvent, None]` (`:30`), but `handle` is annotated
14: `event: NewChatCreatedEvent` (`:31`). Because `publish` dispatches by `event.__class__` and the
15: handler body uses `event.event_id` (present on the base `BaseEvent`), it *works at runtime today*, but
16: the type is wrong and will mislead readers/type-checkers.
17: 
18: **Fix:** change `:31` to `async def handle(self, event: ListenerAddedEvent) -> None:`.
19: 
20: ## #2 — `send_message` argument order mismatch (base vs Kafka)
21: **File:** `app/infrastructure/message_brokers/base.py:20` vs `kafka.py:15`
22: **Severity:** Low (benign)
23: 
24: ABC signature is `send_message(self, topic: str, key: bytes, value: bytes)`; the Kafka implementation
25: is `send_message(self, key: bytes, topic: str, value: bytes)`. All callers in
26: `app/logic/events/messages.py` use **keyword arguments**, so no runtime break. Fragile if anyone ever
27: calls positionally.
28: 
29: **Fix:** align the Kafka implementation to `(self, topic, key, value)`.
30: 
31: ## #3 — `BaseConnectionManager` registered twice
32: **File:** `app/logic/init.py:122` and `:227`
33: **Severity:** Low
34: 
35: Same `ConnectionManager()` singleton registered at both lines. Harmless (identical instance), but
36: redundant and a copy-paste trap. See also `docs/di-reference.md`.
37: 
38: **Fix:** delete one registration (prefer keeping `:122`, which is before `init_mediator` runs).
39: 
40: ## #5 — Inbound Kafka→WebSocket relay is never started
41: **Files:** `app/infrastructure/message_brokers/kafka.py:26` (`start_consuming`); `app/application/api/lifespan.py`
42: **Severity:** Medium (dead code path)
43: 
44: `KafkaMessageBroker.start_consuming` exists and `NewMessageReceivedFromBrokerEvent` /
45: `...Handler` are registered, but the app lifespan (`lifespan.py`) only `start()`s / `close()`s the
46: broker — it never starts a consumer loop that pushes broker messages to WebSocket clients. The
47: inbound path is dead unless an external consumer drives it (e.g. `app/kafka_test_consumer.py`).
48: 
49: **Fix (when implementing):** start the consumer in `lifespan.py` (background task) that yields
50: messages and publishes `NewMessageReceivedFromBrokerEvent` to the mediator. See `CLAUDE.md` →
51: "Known gotchas".
52: 
53: ## #6 — Memory repo implements only 3 of 7 `BaseChatsRepository` methods
54: **Files:** `app/infrastructure/repositories/messages/base.py` (7 abstract methods) vs `memory.py:10`–`33` (3 implemented)
55: **Severity:** Medium (test gap)
56: 
57: `MemoryChatRepository` implements `get_chat_by_oid`, `check_chat_exists_by_title`, `add_chat`. The
58: other four — `get_all_chats`, `delete_chat_by_oid`, `add_telegram_listener`, `get_all_chat_listeners`
59: — are missing. Any test that exercises `DeleteChat`, `GetAllChats`, `AddTelegramListener`, or
60: `GetAllChatsListeners` against the memory repo will raise `NotImplementedError`.
61: 
62: **Fix (when adding a test for those paths):** implement the missing methods in `memory.py` (mirror the
63: Mongo behavior in `mongo.py`), or document that those handlers are not yet memory-testable.
64: 
65: ## #7 — `init_dummy_container` only overrides `BaseChatsRepository`
66: **Files:** `app/test/fixtures.py:10`–`14`
67: **Severity:** Medium (test gap)
68: 
69: `init_dummy_container` rebinds `BaseChatsRepository` → `MemoryChatRepository` but **not**
70: `BaseMessagesRepository`. Tests that post messages / read messages will resolve the Mongo-backed
71: `MongoDBMessagesRepository` and hit real Mongo unless they override it manually.
72: 
73: **Fix (when adding message tests):** also register the memory messages repository (a
74: `MemoryMessagesRepository` does not yet exist — create one mirroring `MemoryChatRepository`, or
75: override `BaseMessagesRepository` in the test's fixture).
76: 
77: ## #8 — Command handlers not uniformly top-level registered
78: **File:** `app/logic/init.py:102`–`109` vs `:136`–`163`
79: **Severity:** Low
80: 
81: `CreateChatCommandHandler`, `CreateMessageCommandHandler`, and all query handlers are top-level
82: `container.register`'d. `DeleteChatCommandHandler` and `AddTelegramListenerCommandHandler` are only
83: manually instantiated inside `init_mediator`. `container.resolve(DeleteChatCommandHandler)` would
84: fail. See `docs/di-reference.md`.
85: 
86: **Fix (when adding a handler):** always add a top-level `container.register(<X>Handler)` so both the
87: container and `init_mediator` can build it.

(End of file - total 99 lines)

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

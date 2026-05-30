# ADR-001: WebSocket Kafka Relay Inbound Consumer

**Status**: Implemented ✅ (verified 2026-09-10)  
**Created**: 2026-09-05  
**Context**: Multi-user chat backend (DDD, CQRS, event-driven architecture)  
**Owner**: @nekits \

> **✅ Implementation Status (2026-09-10):** DONE via `0005-websocket-kafka-relay-implementation-plan.md`.
> `start_kafka_consumer` / `stop_kafka_consumer` run in the app lifespan
> (`app/application/api/main.py`), the consumer loop in `app/application/api/lifespan.py`
> wraps each broker message into `NewMessageReceivedFromBrokerEvent` and publishes it
> to the mediator (as `[event]` — `publish` expects an iterable), and the handler
> broadcasts via `connection_manager.send_all`. Note: the implementation deviates from
> this ADR's sketches on purpose — the shipped code uses the existing async-generator
> `KafkaMessageBroker.start_consuming(topic)` instead of a background thread, and the
> event carries `message`/`chat_oid` instead of `event_id`/`occurred_at`.

## Background

### Current State

The system has:
- Kafka message broker consuming commands and publishing domain events
- WebSocket connections for real-time chat
- Event handlers registered for domain events
- **BUT**: No active consumer loop to bridge Kafka messages to WebSocket events during normal runtime

### The Gap

Per `docs/known-issues.md` issue #5:
> `KafkaMessageBroker.start_consuming` exists and `NewMessageReceivedFromBrokerEvent` / `...Handler` are registered, but the app lifespan (`lifespan.py`) only `start()`s / `close()`s the broker — it never starts a consumer loop that pushes broker messages to WebSocket clients.

### Problem Statement

Without an inline consumer:
1. Commands published to Kafka are not instantly visible to WebSocket clients
2. Clients must rely on server push or polling for message updates
3. Real-time "read latest messages" feature fails
4. Event-driven design is compromised because events never propagate

## Decision Context

| Factor | Options | Decision | Reason |
|--------|---------|----------|--------|
| **Event propagation** | Inline consumer vs External consumer via `NewMessageReceivedFromBrokerEvent` | Inline consumer | Simpler deployment, no external dependency |
| **Latency** | Synchronous (inline) vs Async (background worker) | Synchronous | Chat app requires instant visibility; async adds complexity |
| **Architecture** | Kafka consumer group vs Single broker thread | Single broker thread | Fits CQRS pattern, no coordination complexity |
| **WebSocket relay** | Publish to mediator (current) vs Direct WebSocket push | Current (via mediator) | Consistent with existing event handling pattern |

## Decision: Inline Kafka Consumer within App Lifespan

**We will add a background task to the application lifespan that:**

1. Creates a Kafka consumer subscribing to the chat messages topic
2. Lists connected WebSocket clients
3. Continuously reads messages from Kafka
4. For each message:
   - Fetches the chat UUID from the message body
   - Finds all connected clients for that chat
   - Broadcasts the message via the existing `NewMessageReceivedFromBrokerEvent` handler
   - Publishes to the mediator to trigger WebSocket updates

### Architecture

```
┌─────────────────┐                              ┌─────────────────────┐
│   Kafka Broker  │───────────────┬──────────────│   Application       │
│   (External)    │  messages:    │              │                     │
│  chat-messages- │  topic        │              │  ┌──────────────────┐
│                  └──────────────↓──────┬────────│  ┌──────────────────┐
│                                 Consumer  │  │   Lifespan Handler    │
└─────────────────┤  (Background thread)  │  │     (app/lifespan.py)  │
                  ┌──────────────────────┘  │  └──────────────────────┘
                  │                           │                          ↓
                  │                           │              ┌─────────────────┐
                  │                           │              │   WebSocket     │
                  │                           │    +--------->│ Manager        │
                  │                           │              └─────────────────┘
                  │                           │                          ↓
                  │                            └────────────────────────────→ Mediator
                  │                                                          (event handlers → WebSocket push)
```

### Implementation Details

#### 1. Consumer Configuration

```python
# app/infrastructure/message_brokers/kafka.py

CHAT_MESSAGES_TOPIC = "chat-messages"
CONSUMER_GROUP = "chat-relay"

async def start_consuming(self) -> None:
    """
    Background Kafka consumer thread that bridges broker → WebSocket.
    
    Must be called from asyncio run() or created as a background thread.
    """
    consumer = KafkaConsumer(
        [CHAT_MESSAGES_TOPIC],
        topic partitions=['0', '1', '2'],
        bootstrap_servers=self.config.get('bootstrap_servers', 'kafka:9092'),
        group_id=CONSUMER_GROUP,
        auto_offset_reset='earliest',
        value_deserializer=json.loads,
        key_deserializer=uuid4,
    )
    
    # Initial fetch to get assignments
    consumer.assign(consumer.assignment())
    while True:
        try:
            msg = yield from consumer.poll(timeout=0.5)
            if msg and 'value' in msg:
                await self._relay_message(msg)
        except Exception:
            yield from consumer.poll(timeout=5)
            continue
    """
```

#### 2. Message Relay Handler

```python
async def _relay_message(self, msg: Message) -> None:
    """
    Relays a Kafka message to connected WebSocket clients.
    """
    chat_uuid = msg.key.hex  # UUID from message key
    
    # Get WebSocket clients for this chat via mediator
    clients = await self._mediator.send(
        GetChatMessagesQuery(
            chat_uid=chat_uuid,
            limit=1000,  # Fetch pending messages for WebSocket clients
            include_sender=True
        )
    )['chats']
    
    # Publish event to trigger WebSocket broadcasts
    # This uses the existing event handling pipeline
    await self._mediator.publish(
        NewMessageReceivedFromBrokerEvent(
            event_id=uuid4(),
            occurred_at=datetime.now(),
            title="New message from Kafka relay",
        )
    )
```

#### 3. WebSocket Event Handler Extension

```python
# app/logic/events/messages.py

@dataclass
class NewMessageReceivedFromBrokerEvent(BaseEvent):
    event_title: ClassVar[str] = "NewMessageReceivedFromBrokerEvent"
    event_id: UUID = field(default_factory=uuid4())
    occurred_at: datetime = field(default_factory=datetime.now())

@dataclass
class NewMessageReceivedFromBrokerEventHandler(EventHandler[
    NewMessageReceivedFromBrokerEvent, None
]):
    connection_manager: BaseConnectionManager
    
    def handle(self, event: NewMessageReceivedFromBrokerEvent) -> None:
        """Broadcast new message to all connected WebSocket clients."""
        self.connection_manager.push_message(
            event.event_id,
            NewMessageReceivedFromBrokerEventPayload(
                event=event,
                sender_id="relay",
            )
        )
```

#### 4. Lifecycle Management

```python
# app/application/api/lifespan.py

async def on_event(lifespan: Lifespan) -> None:
    """Start Kafka relay consumer when app starts."""
    broker = container.resolve(KafkaMessageBroker)
    broker.start_consuming()  # Creates and starts background thread

async def on_shutdown(lifespan: Lifespan) -> None:
    """Dispose consumer when app shuts down."""
    broker = container.resolve(KafkaMessageBroker)
    broker.stop_consuming()  # Gracefully close consumer
```

## Alternatives Considered

| Alternative | Pros | Cons |
|--------------|------|------|
| **Inline Kafka consumer** | Simple, no external processes, instant propagation | Threads, harder to debug, potential blocking |
| **External sidecar consumer** | Non-blocking, easy to observe, scalable | More infrastructure, network latency |
| **SSE server-sent events** | Simple alternative to WebSocket | Doesn't support push notifications |
| **Polling REST API** | Simplest implementation | High latency, server load, bad UX |

## Why Inline Consumer Wins

1. **Zero additional infrastructure** — No sidecar containers or pods
2. **Real-time guarantees** — Events propagate immediately
3. **Event-driven integrity** — Uses existing mediator pattern
4. **Simple deployment** — Only one process to configure
5. **Debuggable** — Single point of failure to observe

## Verification Checklist

- [x] Consumer task starts on app warmup (`lifespan` in `app/application/api/main.py`)
- [x] Messages published to the topic are picked up by the consumer loop
- [x] WebSocket clients in the chat receive the message (`NewMessageReceivedFromBrokerEventHandler` → `send_all`)
- [x] Consumer stops gracefully on shutdown (`stop_kafka_consumer` cancels the task + unsubscribes)
- [ ] Message pagination works (clients can catch up)
- [ ] Consumer handles offset rebalancing gracefully
- [ ] No memory leaks in background task on long-run (24h+) — needs soak testing

> Checked items verified by the unit/integration suite (`test_lifespan_outbox_broker.py`,
> `test_messages_api.py` WebSocket tests); unchecked items require a live Kafka environment.

## Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| Thread blocking on slow messages | Configure `max_poll_ms` appropriately (e.g., 500ms) |
| Consumer thread crash | Graceful shutdown via lifespan handler |
| Memory leak from message buffering | Use streaming, don't store messages |
| Kafka consumer group coordination overhead | Use auto-assignment for simplicity |

## References

- `docs/known-issues.md` #5 — Original issue
- `CLAUDE.md` — Known gotchas section |
- `app/application/api/lifespan.py` — Lifespan handler pattern |
- `app/logic/events/messages.py` — Event handler pattern |
- Kafka Python client documentation

---
*Status: Ready for review and implementation*

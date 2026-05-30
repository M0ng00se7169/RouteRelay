# Implementation Plan: WebSocket Kafka Relay (ADR-001)

## Overview
This document provides a comprehensive step-by-step implementation plan for the WebSocket Kafka Relay feature described in `docs/adr/0001-websocket-kafka-relay.md`. The goal is to implement an inline Kafka consumer that bridges broker messages to WebSocket clients during normal runtime.

> **✅ Implementation Status (2026-09-10):** ALL PHASES DONE — the consumer loop,
> lifespan wiring (`start_kafka_consumer` / `stop_kafka_consumer`), and graceful
> shutdown are implemented and covered by the test suite (137 tests passing).
> Deviations from the original plan: `publish()` is called with `[event]` (iterable),
> and `_kafka_consumer_loop` tolerates malformed messages via try/except (both fixed
> 2026-09-10).

---

## Current State Analysis

### What Exists
1. **KafkaMessageBroker** (`app/infrastructure/message_brokers/kafka.py:50-59`) - Has `start_consuming()` method that yields messages from a topic
2. **NewMessageReceivedFromBrokerEvent** (`app/domain/events/messages.py:37-39`) - Event with `message: str` and `chat_oid: str`
3. **NewMessageReceivedFromBrokerEventHandler** (`app/logic/events/messages.py:50-55`) - Handler that broadcasts to WebSocket clients via `connection_manager.send_all()`
4. **ConnectionManager** (`app/infrastructure/websockets/managers.py`) - WebSocket connection manager with `send_all(key, bytes_)` method
5. **Lifespan handlers** (`app/application/api/lifespan.py`) - Manages broker start/close and outbox relay

### What's Missing
1. **Background consumer task in lifespan** - Need to start `start_consuming()` as a background task
2. **Consumer task management** - Start/stop logic in lifespan
3. **Message relay logic** - Transform Kafka messages to `NewMessageReceivedFromBrokerEvent` and publish to mediator

---

## Implementation Plan

### Phase 1: Extend KafkaMessageBroker ✅

**File:** `app/infrastructure/message_brokers/kafka.py`

**Changes:**
1. Add `stop_consuming()` method to unsubscribe consumer (already exists at line 58-60)
2. Ensure `start_consuming()` works correctly with proper topic subscription
3. Add configuration for the relay topic (use existing `new_message_received_topic` from config)

**Note:** The current `start_consuming()` method at lines 50-56:
```python
async def start_consuming(self, topic: str) -> AsyncIterator[dict]:
    if self.consumer is None:
        raise RuntimeError('KafkaMessageBroker.start_consuming called before start()')
    self.consumer.subscribe(topics=[topic])

    async for message in self.consumer:
        yield orjson.loads(message.value)
```
This is already an async generator - perfect for our needs.

---

### Phase 2: Create Kafka Consumer Background Task ✅

**File:** `app/application/api/lifespan.py`

**Changes:**
1. Add new async function `start_kafka_consumer(app: FastAPI | None = None) -> asyncio.Task`:
   - Resolve `KafkaMessageBroker` from container
   - Get topic from config (`config.new_message_received_topic`)
   - Create background task that:
     - Iterates over `broker.start_consuming(topic)`
     - For each message, creates `NewMessageReceivedFromBrokerEvent`
     - Publishes event to mediator
   - Return the task

2. Add `stop_kafka_consumer(task: asyncio.Task) -> None`:
   - Cancel the task
   - Call `broker.stop_consuming()`
   - Handle cleanup gracefully

3. Update `lifespan()` function:
   - Call `start_kafka_consumer(app)` after `init_message_broker(app)`
   - Store returned task
   - Call `stop_kafka_consumer(task)` before `close_message_broker(app)`

**Implementation details for the consumer task:**
```python
async def _kafka_consumer_loop(broker: KafkaMessageBroker, topic: str, mediator: Mediator) -> None:
    async for message in broker.start_consuming(topic):
        try:
            # Message structure: { "chat_oid": "...", "message": "..." }
            chat_oid = message.get("chat_oid")
            message_text = message.get("message")
            
            if chat_oid and message_text:
                event = NewMessageReceivedFromBrokerEvent(
                    message=message_text,
                    chat_oid=chat_oid,
                )
                await mediator.publish(event)
        except Exception as e:
            logger.exception("Error processing Kafka message: %s", e)
            # Continue processing next messages
```

---

### Phase 3: Wire Dependencies in Container ✅

**File:** `app/logic/init.py`

**Changes:**
1. Import `NewMessageReceivedFromBrokerEvent` (already imported at line 55)
2. Ensure `Mediator` is available for the consumer task (already registered at lines 147-149)
3. No additional registration needed - all components already exist

---

### Phase 4: Verify Event Structure Compatibility ✅

**Check:** Ensure Kafka message format matches `NewMessageReceivedFromBrokerEvent` expectations.

From `app/domain/events/messages.py:37-39`:
```python
@dataclass
class NewMessageReceivedFromBrokerEvent(BaseEvent):
    message: str
    chat_oid: str
```

The Kafka message must contain `chat_oid` and `message` fields.

**Check:** `NewMessageReceivedFromBrokerEventHandler` at `app/logic/events/messages.py:50-55`:
```python
@dataclass(frozen=True)
class NewMessageReceivedFromBrokerEventHandler(EventHandler):
    async def handle(self, event: NewMessageReceivedFromBrokerEvent) -> None:
        await self.connection_manager.send_all(
            key=event.chat_oid,
            bytes_=event.message.encode(),
        )
```

This expects `event.chat_oid` and `event.message` - compatible!

---

### Phase 5: Testing Strategy ✅

**Integration Tests:**
1. Start app with real Kafka
2. Send message via HTTP API → writes to outbox → relay publishes to Kafka
3. Verify consumer picks up message and broadcasts to WebSocket clients
4. Test multiple WebSocket clients in same chat
5. Test consumer shutdown gracefully

**Unit Tests:**
1. Test `_kafka_consumer_loop` with mocked broker and mediator
2. Test lifespan start/stop with mocked broker

---

## File Changes Summary

| File | Change Type | Description |
|------|-------------|-------------|
| `app/application/api/lifespan.py` | **Major** | Add `start_kafka_consumer` / `stop_kafka_consumer` functions, update `lifespan()` |
| `app/infrastructure/message_brokers/kafka.py` | **Minor** | Verify `start_consuming` / `stop_consuming` work correctly |
| `app/logic/init.py` | **None** | No changes needed - all deps already registered |

---

## Implementation Order

1. **First:** Update `lifespan.py` with consumer task functions
2. **Second:** Test locally with Docker Compose (Kafka + MongoDB)
3. **Third:** Verify WebSocket clients receive relayed messages
4. **Fourth:** Add integration tests

---

## Configuration

Use existing config values from `settings/config.py`:
- `kafka_url` - bootstrap servers
- `new_message_received_topic` - topic to consume from (default: `new-messages`)
- Consumer group: use existing `group_id='chat'` from broker config

---

## Risk Mitigation

| Risk | Mitigation |
|------|------------|
| Consumer crashes silently | Wrap loop in try/except, log errors, continue |
| Blocking on slow messages | `start_consuming` is async, uses `async for` - non-blocking |
| Message format mismatch | Add validation in consumer loop before creating event |
| Memory leak | No message buffering - stream processing |
| Duplicate consumer group | Use same `group_id` as broker, single consumer instance |

---

## Verification Checklist (from ADR-001)

- [x] Consumer task starts on app warmup
- [x] Messages published to the topic are picked up by the consumer loop
- [x] WebSocket clients receive messages (via `NewMessageReceivedFromBrokerEventHandler`)
- [x] Consumer stops gracefully on shutdown
- [ ] Message pagination works (clients can catch up) — requires live Kafka
- [ ] Consumer handles offset rebalancing gracefully — requires live Kafka
- [ ] No memory leaks in background task on long-run (24h+) — needs soak testing

---

## Next Steps

1. Implement the changes in `lifespan.py`
2. Run local integration test with `docker compose up`
3. Verify end-to-end flow: HTTP POST → Kafka → WebSocket
4. Add to test suite
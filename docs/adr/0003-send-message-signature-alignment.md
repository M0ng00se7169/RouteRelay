# ADR-003: Send Message Signature Alignment

**Status**: Implemented ✅ (verified 2026-09-10)
**Created**: 2026-09-05  
**Context**: Multi-user chat backend (Kafka, message brokers)  
**Owner**: @nekits \

> **✅ Implementation Status (2026-09-10):** DONE. `KafkaMessageBroker.send_message`
> is now `async def send_message(self, topic: str, key: bytes, value: bytes)`
> (`app/infrastructure/message_brokers/kafka.py`), matching the ABC in
> `app/infrastructure/message_brokers/base.py`. Verified by the full suite
> (`cd app && poetry run pytest` — 137 passed), including
> `test_kafka_send_message_uses_producer`.

## Background

### Current State

Per `docs/known-issues.md` issue #2:

> ABC signature is `send_message(self, topic: str, key: bytes, value: bytes)`;
> the Kafka implementation is `send_message(self, key: bytes, topic: str, value: bytes)`.
> All callers in `app/logic/events/messages.py` use **keyword arguments**, so no runtime break.
> Fragile if anyone ever calls positionally.

### Problem Statement

**Inconsistency**: The abstract base class defines a signature, but the concrete Kafka
implementation uses a different (but functionally equivalent) parameter order.

```
# app/infrastructure/message_brokers/base.py:20  ← ABC signature
abc.BaseMessageBroker.send_message(self, topic: str, key: bytes, value: bytes) 

# app/infrastructure/message_brokers/kafka.py:15  ← Concrete implementation  
kafka.KafkaMessageBroker.send_message(self, key: bytes, topic: str, value: bytes)
```

**Root Cause**: Developer prioritized argument ordering for convenience over consistency
with the ABC definition.

## Decision

**Align KafkaMessageBroker.send_message() signature with the ABC definition:**

## Implementation

### File: `app/infrastructure/message_brokers/kafka.py:15`

The `send_message` method signature should change to match the ABC:

```python
# BEFORE (inconsistent)
@overload
def send_message(self, key: bytes, topic: str, value: bytes) -> None:
    ...\n
def send_message(self, key: bytes, topic: str, value: bytes) -> None:\n    ...

# AFTER (aligned with ABC)
@overload
def send_message(self, topic: str, key: bytes, value: bytes) -> None:
    ...\n
def send_message(self, topic: str, key: bytes, value: bytes) -> None:\n    ...
```

Since existing callers use keyword arguments, this change is safe:

```python
# Callers (already safe via keyword args)
kafka.send_message(topic="chat-messages", key=b"uuid-123", value=encoded_msg)
```

## Verification

```python
from application.infrastructure.message_brokers.kafka import KafkaMessageBroker
from application.domain.events.base import BaseEvent

broker = KafkaMessageBroker()
# Now correctly typed - matches ABC
topic = "test"
event = some_event()
broker.send_message(topic=topic, key=event.event_id.value, value=event.encode())
```

## Impact

| Aspect | Impact |
|--------|--------|
| **Runtime behavior** | None — same keyword argument semantics |
| **Type safety** | Improved — signature matches ABC contract |
| **Documentation** | Consistent — docstring now matches actual signature |
| **Breaking changes** | None — keyword argument calls unchanged |
| **API surface** | Aligned with abstract interface |

## References

- `app/infrastructure/message_brokers/base.py:20` — ABC definition |
- `app/infrastructure/message_brokers/kafka.py:15` — file to edit |
- `docs/known-issues.md` #2 — original issue |

---
*Status: Trivial fix, ready for code review*

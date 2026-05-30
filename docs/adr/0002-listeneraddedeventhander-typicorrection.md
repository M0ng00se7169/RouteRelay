# ADR-002: Type Correction - ListenerAddedEventHandler

**Status**: Implemented ✅ (verified 2026-09-10)
**Created**: 2026-09-05  
**Context**: Multi-user chat backend (DDD, CQRS, event-driven architecture)  
**Owner**: @nekits \

> **✅ Implementation Status (2026-09-10):** DONE. `handle` is annotated
> `async def handle(self, event: ListenerAddedEvent) -> None` in
> `app/logic/events/messages.py`. The handler has since also gained the optional
> `notification_client` field (see `docs/adr/issue4.md`); the type correction
> itself is in place. Covered by `app/test/application/api/test_telegram_integration.py`.

## Background

### Current State

Per `docs/known-issues.md` issue #1:

> The class is bound to `EventHandler[ListenerAddedEvent, None]` (`:30`), but `handle` is annotated
> `event: NewChatCreatedEvent` (`:31`). Because `publish` dispatches by `event.__class__` and the
> handler body uses `event.event_id` (present on the base `BaseEvent`), it *works at runtime today*, but
> the type is wrong and will mislead readers/type-checkers.

### Problem Statement

**File**: `app/logic/events/messages.py:31`

```python
@dataclass
class ListenerAddedEventHandler(EventHandler[ListenerAddedEvent, None]):
    connection_manager: BaseConnectionManager
    broker_topic: str = "listeners"
    
    def handle(self, event: NewChatCreatedEvent) -> None:  # WRONG TYPE
        event_id: str = event.event_id.hex  # Works but should be event_id
```

**Root Cause**: Copy-paste error when extracting `ListenerAddedEventHandler` from `NewChatCreatedEventHandler`.

## Decision

**Change the `handle` method signature to match its binding:**

```python
def handle(self, event: ListenerAddedEvent) -> None:  # CORRECT
```

## Implementation

### File: `app/logic/events/messages.py:31`

```diff
 @dataclass
class ListenerAddedEventHandler(EventHandler[ListenerAddedEvent, None]):
     connection_manager: BaseConnectionManager
     broker_topic: str = "listeners"
-    
-    def handle(self, event: NewChatCreatedEvent) -> None:
+    
+    def handle(self, event: ListenerAddedEvent) -> None:
         event_id: str = event.event_id.hex
```

## Verification

```python
# Before: Incorrect type inference
from application.logic.events.messages import ListenerAddedEventHandler
from application.infrastructure.connections.memory import MemoryConnectionManager

listener = ListenerAddedEventHandler(MemoryConnectionManager())
assert listener.__annotations__['handle'][1] == ListenerAddedEvent
assert listener.__annotations__['handle'][1].name == 'event'
# Type checker passes, documentation is accurate
```

## Impact

| Aspect | Impact |
|--------|--------|
| **Runtime behavior** | None — event ID extraction still works |
| **Type safety** | Improved — type checker will now enforce correct type |
| **Documentation** | Accurate — docstring now matches implementation |
| **Breaking changes** | None — signature matches the class binding |
| **Dependencies** | None — no external changes required |

## References

- `app/logic/events/messages.py` — file to edit |
- `docs/known-issues.md` #1 — original issue |
- `app/logic/events/base.py` — EventHandler ABC |

---
*Status: Trivial fix, ready for code review*

# ADR-004: Duplicate ConnectionManager Registration Removal

**Status**: Implemented ✅ (verified 2026-09-10)
**Created**: 2026-09-05  
**Context**: Multi-user chat backend (Dependency Injection, DI container)  
**Owner**: @nekits \

> **✅ Implementation Status (2026-09-10):** DONE. `BaseConnectionManager` is
> registered exactly once in `app/logic/init.py` (`container.register(BaseConnectionManager,
> instance=ConnectionManager(), scope=Scope.singleton)`); the second registration
> is gone. All event handlers resolve the same singleton, and the full test suite
> passes.

## Background

### Current State

Per `docs/known-issues.md` issue #3:

> Same `ConnectionManager()` singleton registered at both lines.
> Harmless (identical instance), but redundant and a copy-paste trap.

### Problem Statement

**File**: `app/logic/init.py:122` and `:227`

The same `ConnectionManager` singleton instance is registered twice in the DI container.
While this is harmless (returns the same cached instance), this represents:

1. **Dead code** — unnecessary duplication
2. **Maintenance burden** — one of these won't be there going forward
3. **Copy-paste error risk** — easy accidental omission later
4. **Confusion** — why is it registered twice?

### Technical Context

The current registration looks like:

```python
# Line ~100: Command handlers
container.register(CreateChatCommand, handler)
container.register(CreateMessageCommand, handler)
# ...

# Line ~122: Connection manager (FIRST registration)
container.register(ConnectionManager(), ConnectionManager())
mediator = container.resolve(EventMediator)
# ...

# Line ~227: Connection manager (SECOND registration)
container.register(ConnectionManager(), ConnectionManager())  # DUPLICATE!
```

## Decision

**Remove the duplicate registration at line 227.**

**Rationale**: The first registration at line 122 runs before `init_mediator`, and the
connection manager is needed by event handlers (which are registered after the mediator).
The second registration serves no purpose.

## Implementation

### File: `app/logic/init.py:227` — DELETE

Remove this line:

```python
# UNCOMMENT TO VERIFY:
# container.register(ConnectionManager(), ConnectionManager())  # DUPLICATE - DELETE
```

Or directly delete the registration line that appears second (count from top of file).

## Verification

1. Single registration confirmed:

```python
containers = list(container._registry.keys())
assert containers.count('ConnectionManager') == 1
```

2. Services still accessible:

```python
container.resolve(ConnectionManager)  # Works
container.resolve(EventMediator)  # Works
container.resolve(NewMessageReceivedEventHandler)  # Works (it needs ConnectionManager)
```

3. No duplicate in debug output:

```
Container registry: [5 items]
  CreateChatCommand
  CreateMessageCommand
  ConnectionManager
  EventMediator
  ...

No duplicate entries present ✓
```

## Impact

| Aspect | Impact |
|--------|--------|
| **Runtime behavior** | None — same cached instance returned |
| **Container size** | Reduced by 1 entry |
| **Code quality** | Improved — removed redundancy |
| **Maintenance** | Improved — fewer places to update if needed |
| **Breaking changes** | None |

## References

- `app/logic/init.py:122` — first registration (keep)
- `app/logic/init.py:227` — second registration (delete) |
- `docs/known-issues.md` #3 — original issue |
- `docs/di-reference.md` — DI container patterns |

---
*Status: Trivial fix, ready for code review*

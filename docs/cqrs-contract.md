# CQRS Contract

The exact signatures the CQRS + Mediator layers require. `CLAUDE.md` describes the shapes in prose;
this is the enforceable contract reference.

## Base classes

### Command (`app/logic/commands/base.py`)

```python
@dataclass(frozen=True)
class BaseCommand(ABC): ...

CT = TypeVar('CT', bound=BaseCommand)
CR = TypeVar('CR', bound=Any)

@dataclass(frozen=True)
class CommandHandler(ABC, Generic[CT, CR]):
    _mediator: EventMediator

    @abstractmethod
    async def handle(self, command: CT) -> CR: ...
```

### Query (`app/logic/queries/base.py`)

```python
@dataclass(frozen=True)
class BaseQuery(ABC): ...

QT = TypeVar('QT', bound=BaseQuery)
QR = TypeVar('QR', bound=Any)

@dataclass(frozen=True)
class BaseQueryHandler(ABC, Generic[QT, QR]):
    @abstractmethod
    async def handle(self, query: QT) -> QR: ...
```

### Event / EventHandler (`app/logic/events/base.py`)

```python
ET = TypeVar('ET', bound=BaseEvent)
ER = TypeVar('ER', bound=Any)

@dataclass
class IntegrationEvent(BaseEvent, ABC): ...

@dataclass
class EventHandler(ABC, Generic[ET, ER]):
    message_broker: BaseMessageBroker
    connection_manager: BaseConnectionManager
    broker_topic: str | None = None

    @abstractmethod
    def handle(self, event: ET) -> ER: ...
```

> `EventHandler.handle` is declared `def` in the ABC but every concrete handler in
> `app/logic/events/messages.py` implements it as `async def`. Follow the concrete code: make handler
> `handle` **async** and `await` broker/manager calls.

### Domain event base (`app/domain/events/base.py`)

```python
@dataclass
class BaseEvent(ABC):
    event_title: ClassVar[str]
    event_id: UUID = field(default_factory=uuid4, kw_only=True)
    occurred_at: datetime = field(default_factory=datetime.now, kw_only=True)
```

Entities register domain events and expose `pull_events()` to drain them (see `CLAUDE.md` → "rich
domain model").

## Handler construction invariants

| Concern | Rule |
|---|---|
| Command handler | Frozen `@dataclass`. First field **must be** `_mediator: EventMediator`. Dependencies follow. |
| Query handler | Frozen `@dataclass`. No `_mediator` needed (queries don't publish events). |
| Event handler | `@dataclass` (not frozen). Injects `message_broker`, `connection_manager`, `broker_topic`. |
| Command naming | `<X>Command` + `<X>CommandHandler`, both frozen dataclasses. |
| Query naming | `<X>Query` + `<X>QueryHandler`. |
| Event naming | `<X>Event` (domain, in `domain/events/messages.py`) + `<X>EventHandler` (logic). |
| Publish convention | Command handler ends by `await self._mediator.publish(entity.pull_events())`, then returns. (See `app/logic/commands/messages.py:45,69,91,117`.) |

## Mediator dispatch (`app/logic/mediator/base.py`)

- `register_command(command_type, [handler, ...])`, `register_query(query_type, handler)`,
  `register_event(event_type, [handler, ...])`.
- `handle_command` dispatches to **all** handlers registered for the command type; raises
  `CommandHandlersNotRegisteredException` if none (`base.py:69`).
- `publish(events)` dispatches each event to **all** handlers registered for `event.__class__`
  (`base.py:56`–`63`). Events are dispatched **inline, synchronously within the request** — see
  `CLAUDE.md` "Known gotchas" (write latency couples to Kafka).

## Reference implementation

`app/logic/commands/messages.py` and `app/logic/events/messages.py` are the canonical examples to
copy when adding a feature (see `.claude/rules/feature-workflow.md`).

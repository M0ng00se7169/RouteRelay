from collections import defaultdict
from collections.abc import Iterable
from dataclasses import (
    dataclass,
    field,
)

from domain.events.base import BaseEvent
from infrastructure.metrics import safe_inc
from infrastructure.metrics import (
    mediator_commands_handled_total,
    mediator_events_published_total,
    mediator_queries_handled_total,
)
from logic.commands.base import (
    BaseCommand,
    CommandHandler,
    CR,
    CT,
)
from logic.events.base import (
    ER,
    ET,
    EventHandler,
)
from logic.exceptions.mediator import CommandHandlersNotRegisteredException
from logic.mediator.command import CommandMediator
from logic.mediator.event import EventMediator
from logic.mediator.query import QueryMediator
from logic.queries.base import (
    BaseQuery,
    BaseQueryHandler,
    QR,
    QT,
)


@dataclass(eq=False)
class Mediator(EventMediator, QueryMediator, CommandMediator):
    events_map: dict[ET, EventHandler] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )
    commands_map: dict[CT, CommandHandler] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )
    queries_map: dict[QT, BaseQueryHandler] = field(
        default_factory=dict,
        kw_only=True,
    )

    def register_event(self, event: ET, event_handlers: Iterable[EventHandler[ET, ER]]):
        self.events_map[event].extend(event_handlers)

    def register_command(self, command: CT, command_handlers: Iterable[CommandHandler[CT, CR]]):
        self.commands_map[command].extend(command_handlers)

    def register_query(self, query: QT, query_handler: BaseQueryHandler[QT, QR]) -> QR:
        self.queries_map[query] = query_handler

    async def publish(self, events: Iterable[BaseEvent]) -> Iterable[ER]:
        result = []

        for event in events:
            # Flow-volume counter (ADR-0006, Chunk 5.1): one increment per event
            # dispatched, labelled by class name — a closed, small set (D3).
            safe_inc(mediator_events_published_total, event=event.__class__.__name__)
            handlers: Iterable[EventHandler] = self.events_map[event.__class__]
            result.extend([await handler.handle(event) for handler in handlers])

        return result

    async def handle_command(self, command: BaseCommand) -> Iterable[CR]:
        command_type = command.__class__
        handlers = self.commands_map.get(command_type)

        if not handlers:
            raise CommandHandlersNotRegisteredException(command_type)

        # Counted after handler resolution so an unregistered command (which
        # raises below the .get()) is never counted as handled.
        safe_inc(mediator_commands_handled_total, command=command_type.__name__)
        return [await handler.handle(command) for handler in handlers]

    async def handle_query(self, query: BaseQuery) -> QR:
        safe_inc(mediator_queries_handled_total, query=query.__class__.__name__)
        return await self.queries_map[query.__class__].handle(query=query)

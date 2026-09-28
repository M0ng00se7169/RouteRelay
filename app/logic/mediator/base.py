from collections import defaultdict
from collections.abc import Iterable
from dataclasses import (
    dataclass,
    field,
)
from typing import Any

from domain.events.base import BaseEvent
from infrastructure.metrics import (
    mediator_commands_handled_total,
    mediator_events_published_total,
    mediator_queries_handled_total,
    safe_inc,
)
from logic.commands.base import BaseCommand, CommandHandler
from logic.events.base import EventHandler
from logic.exceptions.mediator import CommandHandlersNotRegisteredException
from logic.mediator.command import CommandMediator
from logic.mediator.event import EventMediator
from logic.mediator.query import QueryMediator
from logic.queries.base import BaseQuery, BaseQueryHandler


@dataclass(eq=False)
class Mediator(EventMediator[BaseEvent, Any], QueryMediator[BaseQuery, Any], CommandMediator[BaseCommand, Any]):
    # Value types are parameterized with Any (not BaseEvent/BaseCommand):
    # handlers are invariant in their message type, so only Any accepts the
    # heterogeneous concrete handlers registered on one mediator instance.
    events_map: dict[type[BaseEvent], list[EventHandler[Any, Any]]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )
    commands_map: dict[type[BaseCommand], list[CommandHandler[Any, Any]]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )
    queries_map: dict[type[BaseQuery], BaseQueryHandler[Any, Any]] = field(
        default_factory=dict,
        kw_only=True,
    )

    def register_event(
        self,
        event: type[BaseEvent],
        event_handlers: Iterable[EventHandler[Any, Any]],
    ) -> None:
        self.events_map[event].extend(event_handlers)

    def register_command(
        self,
        command: type[BaseCommand],
        command_handlers: Iterable[CommandHandler[Any, Any]],
    ) -> None:
        self.commands_map[command].extend(command_handlers)

    def register_query(
        self,
        query: type[BaseQuery],
        query_handler: BaseQueryHandler[Any, Any],
    ) -> None:
        self.queries_map[query] = query_handler

    async def publish(self, events: Iterable[BaseEvent]) -> list[Any]:
        result: list[Any] = []

        for event in events:
            # Flow-volume counter (ADR-0006, Chunk 5.1): one increment per event
            # dispatched, labelled by class name — a closed, small set (D3).
            safe_inc(mediator_events_published_total, event=event.__class__.__name__)
            handlers: list[EventHandler[Any, Any]] = self.events_map[event.__class__]
            result.extend([await handler.handle(event) for handler in handlers])

        return result

    async def handle_command(self, command: BaseCommand) -> list[Any]:
        command_type = command.__class__
        handlers = self.commands_map.get(command_type)

        if not handlers:
            raise CommandHandlersNotRegisteredException(command_type)

        # Counted after handler resolution so an unregistered command (which
        # raises below the .get()) is never counted as handled.
        safe_inc(mediator_commands_handled_total, command=command_type.__name__)
        return [await handler.handle(command) for handler in handlers]

    async def handle_query(self, query: BaseQuery) -> Any:
        safe_inc(mediator_queries_handled_total, query=query.__class__.__name__)
        return await self.queries_map[query.__class__].handle(query=query)

from abc import (
    ABC,
    abstractmethod,
)
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import (
    dataclass,
    field,
)
from typing import Generic

from domain.events.base import BaseEvent
from logic.events.base import (
    ER,
    ET,
    EventHandler,
)


@dataclass(eq=False)
class EventMediator(ABC, Generic[ET, ER]):
    # Events are dispatched by class, so the map is keyed by the event TYPE
    # (type[ET]) while handlers are parameterized on the event itself.
    events_map: dict[type[ET], list[EventHandler[ET, ER]]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )

    @abstractmethod
    def register_event(
        self,
        event: type[ET],
        event_handlers: Iterable[EventHandler[ET, ER]],
    ) -> None:
        ...

    @abstractmethod
    async def publish(self, events: Iterable[BaseEvent]) -> Iterable[ER]:
        ...

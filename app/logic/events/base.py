from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
)

from domain.events.base import BaseEvent
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.websockets.managers import BaseConnectionManager

# NOTE: no PEP 696 `default=` here — that needs Python 3.13 at runtime and
# this project supports 3.11+. Concrete handlers/mediators are parameterized
# explicitly at their definitions instead.
ET = TypeVar('ET', bound=BaseEvent)
ER = TypeVar('ER')


@dataclass
class IntegrationEvent(BaseEvent, ABC):
    ...


@dataclass(frozen=True)
class EventHandler(ABC, Generic[ET, ER]):
    message_broker: BaseMessageBroker
    connection_manager: BaseConnectionManager
    broker_topic: str | None = None

    # Async: Mediator.publish awaits every handler; ER is the awaited result.
    @abstractmethod
    async def handle(self, event: ET) -> ER:
        ...

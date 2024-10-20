from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Any, Generic

from domain.events.base import BaseEvent

ET = TypeVar('ET', bound=BaseEvent)
ER = TypeVar('ER', bound=Any)


@dataclass
class EventHandler(ABC, Generic[ET, ER]):

    @abstractmethod
    def handle(self, event: ET) -> ER:
        ...

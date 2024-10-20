from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Any, Generic


@dataclass(frozen=True)
class BaseCommand(ABC):
    ...


CT = TypeVar('CT', bound=BaseCommand)
CR = TypeVar('CR', bound=Any)


@dataclass(frozen=True)
class CommandHandler(ABC, Generic[CT, CR]):
    @abstractmethod
    async def handle(self, command: CT) -> CR:
        ...

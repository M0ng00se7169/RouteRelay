from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
)

from logic.mediator.event import EventMediator


# Plain marker class (NOT a dataclass): command DTOs use every dataclass
# flavor, and inheriting a frozen dataclass from a non-frozen one raises
# TypeError at class-creation time. Inheriting a plain class is always safe.
class BaseCommand:
    ...


# NOTE: no PEP 696 `default=` here — that needs Python 3.13 at runtime and
# this project supports 3.11+. Concrete handlers/mediators are parameterized
# explicitly at their definitions instead.
CT = TypeVar('CT', bound=BaseCommand)
CR = TypeVar('CR')


# Non-frozen: the concrete handlers are non-frozen dataclasses, and Python
# forbids a non-frozen dataclass from inheriting a frozen one.
@dataclass
class CommandHandler(ABC, Generic[CT, CR]):
    _mediator: EventMediator

    @abstractmethod
    async def handle(self, command: CT) -> CR:
        ...

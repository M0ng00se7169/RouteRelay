from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import (
    Generic,
    TypeVar,
)


# Plain marker class (NOT a dataclass): query DTOs use every dataclass
# flavor, and mixed frozen/non-frozen dataclass inheritance raises TypeError
# at class-creation time. Inheriting a plain class is always safe.
class BaseQuery:
    ...


# NOTE: no PEP 696 `default=` here — that needs Python 3.13 at runtime and
# this project supports 3.11+. Concrete handlers/mediators are parameterized
# explicitly at their definitions instead.
QT = TypeVar('QT', bound=BaseQuery)
QR = TypeVar('QR')


@dataclass(frozen=True)
class BaseQueryHandler(ABC, Generic[QT, QR]):
    @abstractmethod
    async def handle(self, query: QT) -> QR:
        ...

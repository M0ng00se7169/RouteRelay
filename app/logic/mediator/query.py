from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import (
    dataclass,
    field,
)
from typing import (
    Any,
    Generic,
)

from logic.queries.base import (
    QR,
    QT,
    BaseQuery,
    BaseQueryHandler,
)


@dataclass(eq=False)
class QueryMediator(ABC, Generic[QT, QR]):
    # Queries are dispatched by class, so the map is keyed by the query TYPE
    # (type[QT]) while handlers are parameterized on the query itself.
    queries_map: dict[type[QT], BaseQueryHandler[QT, QR]] = field(
        default_factory=dict,
        kw_only=True,
    )

    @abstractmethod
    def register_query(self, query: type[QT], query_handler: BaseQueryHandler[QT, QR]) -> None:
        ...

    @abstractmethod
    async def handle_query(self, query: BaseQuery) -> Any:
        ...

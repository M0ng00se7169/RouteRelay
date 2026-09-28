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

from logic.commands.base import (
    CR,
    CT,
    CommandHandler,
)


@dataclass(eq=False)
class CommandMediator(ABC, Generic[CT, CR]):
    # Commands are dispatched by class, so the map is keyed by the command
    # TYPE (type[CT]) while handlers are parameterized on the command itself.
    commands_map: dict[type[CT], list[CommandHandler[CT, CR]]] = field(
        default_factory=lambda: defaultdict(list),
        kw_only=True,
    )

    @abstractmethod
    def register_command(
        self,
        command: type[CT],
        command_handlers: Iterable[CommandHandler[CT, CR]],
    ) -> None:
        ...

    @abstractmethod
    async def handle_command(self, command: CT) -> Iterable[CR]:
        ...

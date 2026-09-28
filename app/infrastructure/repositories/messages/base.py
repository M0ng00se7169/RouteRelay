from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters,
    GetMessagesFilters,
)

# Transactions are optional: the test session provider returns None, and the
# Mongo one hands out motor client sessions (see infrastructure/outbox/session.py).
# Repositories only forward the session to motor, so the base signatures keep it
# deliberately loose (Any) — the concrete Mongo implementation narrows it.
SessionHint = Any


@dataclass
class BaseChatsRepository(ABC):

    @abstractmethod
    async def check_chat_exists_by_title(self, title: str) -> bool:
        ...

    @abstractmethod
    async def get_chat_by_oid(self, oid: str) -> Chat | None:
        ...

    @abstractmethod
    async def add_chat(self, chat: Chat, session: SessionHint | None = None) -> None:
        ...

    @abstractmethod
    async def get_all_chats(self, filters: GetAllChatsFilters) -> tuple[list[Chat], int]:
        ...

    @abstractmethod
    async def delete_chat_by_oid(self, chat_oid: str, session: SessionHint | None = None) -> None:
        ...

    @abstractmethod
    async def add_telegram_listener(
        self,
        chat_oid: str,
        telegram_chat_id: str,
        session: SessionHint | None = None,
    ) -> None:
        ...

    @abstractmethod
    async def get_all_chat_listeners(self, chat_oid: str) -> Iterable[ChatListener]:
        ...


@dataclass
class BaseMessagesRepository(ABC):

    @abstractmethod
    async def add_message(self, message: Message, session: SessionHint | None = None) -> None:
        ...

    @abstractmethod
    async def get_messages(self, chat_oid: str, filters: GetMessagesFilters) -> tuple[list[Message], int]:
        ...

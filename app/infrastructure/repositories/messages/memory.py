from dataclasses import (
    dataclass,
    field,
)

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters,
    GetMessagesFilters,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)


@dataclass
class MemoryChatRepository(BaseChatsRepository):
    _saved_chats: list[Chat] = field(default_factory=list, kw_only=True)

    async def get_chat_by_oid(self, oid: str) -> Chat | None:
        try:
            return next(
                chat for chat in self._saved_chats if chat.oid == oid
            )
        except StopIteration:
            return None

    async def check_chat_exists_by_title(self, title: str) -> bool:
        try:
            return bool(
                next(
                    chat for chat in self._saved_chats if chat.title.as_generic_type() == title
                ),
            )
        except StopIteration:
            return False

    async def add_chat(self, chat: Chat, session=None) -> None:
        self._saved_chats.append(chat)

    async def get_all_chats(self, filters: GetAllChatsFilters) -> tuple[list[Chat], int]:
        chats = self._saved_chats[filters.offset:filters.offset + filters.limit]
        return chats, len(self._saved_chats)

    async def delete_chat_by_oid(self, chat_oid: str, session=None) -> None:
        self._saved_chats = [chat for chat in self._saved_chats if chat.oid != chat_oid]

    async def add_telegram_listener(
        self,
        chat_oid: str,
        telegram_chat_id: str,
        session=None,
    ) -> None:
        chat = await self.get_chat_by_oid(oid=chat_oid)
        if chat is None:
            return
        chat.listeners.add(ChatListener(oid=telegram_chat_id))

    async def get_all_chat_listeners(self, chat_oid: str) -> list[ChatListener]:
        chat = await self.get_chat_by_oid(oid=chat_oid)
        if chat is None:
            return []
        return list(chat.listeners)

    async def get_chat_by_title(self, title: str) -> Chat | None:
        try:
            return next(
                chat for chat in self._saved_chats if chat.title.as_generic_type() == title
            )
        except StopIteration:
            return None

    async def bulk_delete_chats(self, chat_oids: list[str]) -> int:
        initial_count = len(self._saved_chats)
        self._saved_chats = [
            chat for chat in self._saved_chats if chat.oid not in chat_oids
        ]
        return initial_count - len(self._saved_chats)


@dataclass
class MemoryMessagesRepository(BaseMessagesRepository):
    _saved_messages: list[Message] = field(default_factory=list, kw_only=True)

    async def add_message(self, message: Message, session=None) -> None:
        self._saved_messages.append(message)

    async def get_messages(
        self,
        chat_oid: str,
        filters: GetMessagesFilters,
    ) -> tuple[list[Message], int]:
        messages = [m for m in self._saved_messages if m.chat_oid == chat_oid]
        count = len(messages)
        paginated = messages[filters.offset:filters.offset + filters.limit]
        return paginated, count

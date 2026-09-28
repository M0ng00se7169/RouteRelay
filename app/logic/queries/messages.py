from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import TypeVar

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from infrastructure.metrics import (
    db_operation_errors_total,
    safe_inc,
)
from infrastructure.presence.base import BasePresenceTracker
from infrastructure.repositories.filters.messages import (
	GetAllChatsFilters,
	GetMessagesFilters,
)
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from logic.exceptions.messages import ChatNotFoundException
from logic.queries.base import (
    BaseQuery,
    BaseQueryHandler,
)

_T = TypeVar('_T')


async def _count_db_errors(operation: str, collection: str, call: Callable[[], Awaitable[_T]]) -> _T:
    """Await a persistence call, counting its exceptions on
    ``db_operation_errors_total`` (ADR-0006, Chunk 5.2) before re-raising.

    Domain errors raised by the handler around the call (e.g.
    ``ChatNotFoundException``) do not pass through here and stay uncounted.
    """
    try:
        return await call()
    except Exception as e:
        safe_inc(
            db_operation_errors_total,
            operation=f'{operation}.{e.__class__.__name__}',
            collection=collection,
            exception=e.__class__.__name__,
        )
        raise


@dataclass(frozen=True)
class GetChatDetailQuery(BaseQuery):
    chat_oid: str


@dataclass(frozen=True)
class GetMessagesQuery(BaseQuery):
    chat_oid: str
    filters: GetMessagesFilters


@dataclass(frozen=True)
class GetAllChatsQuery(BaseQuery):
    filters: GetAllChatsFilters


@dataclass(frozen=True)
class GetAllChatsListenersQuery(BaseQuery):
    chat_oid: str


@dataclass(frozen=True)
class GetChatPresenceQuery(BaseQuery):
    """How many WebSocket clients are currently attached to a chat.

    Presence is API data, not a Prometheus gauge: a per-chat gauge would need a
    chat-oid label, which is unbounded and forbidden (ADR-0006 D3). This is why
    it is a query with an endpoint.
    """

    chat_oid: str


@dataclass(frozen=True)
class GetChatDetailQueryHandler(BaseQueryHandler[GetChatDetailQuery, Chat]):
    chats_repository: BaseChatsRepository
    messages_repository: BaseMessagesRepository

    async def handle(self, query: GetChatDetailQuery) -> Chat:
        chat = await _count_db_errors(
            'query', 'chats',
            lambda: self.chats_repository.get_chat_by_oid(oid=query.chat_oid),
        )

        if not chat:
            raise ChatNotFoundException(chat_oid=query.chat_oid)

        return chat


@dataclass(frozen=True)
class GetMessagesQueryHandler(BaseQueryHandler[GetMessagesQuery, tuple[list[Message], int]]):
    messages_repository: BaseMessagesRepository

    async def handle(self, query: GetMessagesQuery) -> tuple[list[Message], int]:
        return await _count_db_errors(
            'query', 'messages',
            lambda: self.messages_repository.get_messages(
                chat_oid=query.chat_oid,
                filters=query.filters,
            ),
        )


@dataclass(frozen=True)
class GetAllChatsQueryHandler(BaseQueryHandler[GetAllChatsQuery, tuple[list[Chat], int]]):
    chats_repository: BaseChatsRepository

    # Returns the (chats, total) tuple — the API handler unpacks both.
    async def handle(self, query: GetAllChatsQuery) -> tuple[list[Chat], int]:
        return await _count_db_errors(
            'query', 'chats',
            lambda: self.chats_repository.get_all_chats(filters=query.filters),
        )


@dataclass(frozen=True)
class GetAllChatsListenersQueryHandler(BaseQueryHandler[GetAllChatsListenersQuery, Iterable[ChatListener]]):
    chats_repository: BaseChatsRepository

    async def handle(self, query: GetAllChatsListenersQuery) -> Iterable[ChatListener]:
        chat = await _count_db_errors(
            'query', 'chats',
            lambda: self.chats_repository.get_chat_by_oid(oid=query.chat_oid),
        )

        if not chat:
            raise ChatNotFoundException(chat_oid=query.chat_oid)

        return await _count_db_errors(
            'query', 'chats',
            lambda: self.chats_repository.get_all_chat_listeners(chat_oid=query.chat_oid),
        )


@dataclass(frozen=True)
class GetChatPresenceQueryHandler(BaseQueryHandler[GetChatPresenceQuery, int]):
    """Live-socket count for a chat, served from the presence tracker.

    The tracker is the ONLY dependency: presence is ephemeral state that never
    touches Mongo, and it is best-effort by contract — when the feature is off
    the container still resolves a tracker, so this handler reports 0 rather
    than special-casing a missing collaborator.
    """

    presence_tracker: BasePresenceTracker

    async def handle(self, query: GetChatPresenceQuery) -> int:
        return await self.presence_tracker.count(chat_oid=query.chat_oid)
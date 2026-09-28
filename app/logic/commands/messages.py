from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from domain.events.base import BaseEvent
from domain.values.messages import (
    Text,
    Title,
)
from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.keys import (
	chat_cache_key,
	chat_version_key,
)
from infrastructure.metrics import (
	cache_operations_total,
	db_operation_errors_total,
	safe_inc,
)
from infrastructure.outbox.base import BaseOutboxRepository
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.messages.base import (
	BaseChatsRepository,
	BaseMessagesRepository,
)
from logic.commands.base import BaseCommand, CommandHandler
from logic.exceptions.messages import (
    ChatNotFoundException,
    ChatWithThatTitleAlreadyExistsException,
)
from logic.mediator.event import EventMediator

_T = TypeVar('_T')


async def _count_db_errors(operation: str, collection: str, call: Callable[[], Awaitable[_T]]) -> _T:
    """Await a persistence call, counting its exceptions on
    ``db_operation_errors_total`` (ADR-0006, Chunk 5.2) before re-raising.

    Only unexpected persistence failures (Mongo down, timeouts, ...) land here.
    Domain errors (``ChatNotFoundException``, duplicate title, ...) are raised
    by the handlers themselves, outside the wrapped calls, and stay uncounted.
    Metrics are non-fatal (D5): the counter update goes through ``safe_inc``.
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


async def _invalidate_messages_cache(cache: BaseCacheClient, chat_oid: str) -> None:
	"""Bump the chat's cache generation (ADR-0008 Chunk 3).

	Every cached messages page is keyed by this generation, so one INCR makes
	all of them unreachable at once and they expire on their own TTL — no
	SCAN, no wildcard delete.

	Best effort by construction: the cache client's contract already returns a
	safe value on error, and the cost of a missed invalidation is bounded by
	``CACHE_TTL_SECONDS`` of staleness. It is never a lost write.
	"""
	await cache.increment(chat_version_key(chat_oid))
	safe_inc(cache_operations_total, operation='delete', result='ok')


@asynccontextmanager
async def _maybe_transaction(
    session_provider: SessionProvider,
) -> AsyncIterator[object | None]:
    session = await session_provider()
    if session is None:
        yield None
    else:
        async with session.start_transaction():
            yield session


@dataclass
class CreateChatCommand(BaseCommand):
    title: str


@dataclass
class CreateChatCommandHandler(CommandHandler[CreateChatCommand, Chat]):
    _mediator: EventMediator[BaseEvent, Any]
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider

    async def handle(self, command: CreateChatCommand) -> Chat:
        chat_exists = await self.chats_repository.check_chat_exists_by_title(title=command.title)
        if chat_exists:
            raise ChatWithThatTitleAlreadyExistsException(title=command.title)
        new_chat = Chat.create_chat(title=Title(command.title))
        events = new_chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await _count_db_errors(
                'insert', 'chats',
                lambda: self.chats_repository.add_chat(new_chat, session=session),
            )
            await _count_db_errors(
                'insert', 'outbox',
                lambda: self.outbox_repository.save_events(events, session=session),
            )
        await self._mediator.publish(events)
        return new_chat


@dataclass
class CreateMessageCommand(BaseCommand):
    chat_oid: str
    text: str


@dataclass
class CreateMessageCommandHandler(CommandHandler[CreateMessageCommand, Message]):
    _mediator: EventMediator[BaseEvent, Any]
    messages_repository: BaseMessagesRepository
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider
    # ADR-0008: the cache client for write-path invalidation. Always present —
    # with the feature flag off it is the in-memory client, so no branch here.
    cache: BaseCacheClient

    async def handle(self, command: CreateMessageCommand) -> Message:
        chat: Chat | None = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        message: Message = Message(
            chat_oid=command.chat_oid,
            text=Text(command.text),
        )
        chat.add_message(message)
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await _count_db_errors(
                'insert', 'messages',
                lambda: self.messages_repository.add_message(message, session=session),
            )
            await _count_db_errors(
                'insert', 'outbox',
                lambda: self.outbox_repository.save_events(events, session=session),
            )
        # Read-after-write freshness: the next GetMessagesQuery cannot serve the
        # page key cached before this post. Done AFTER the transaction commits so
        # we never invalidate for a write that rolled back.
        await _invalidate_messages_cache(self.cache, command.chat_oid)
        await self._mediator.publish(events)
        return message


@dataclass
class DeleteChatCommand(BaseCommand):
    chat_oid: str


@dataclass
class DeleteChatCommandHandler(CommandHandler[DeleteChatCommand, None]):
    _mediator: EventMediator[BaseEvent, Any]
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider
    # ADR-0008: the cache client for write-path invalidation.
    cache: BaseCacheClient

    async def handle(self, command: DeleteChatCommand) -> None:
        chat: Chat | None = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        chat.delete()
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await _count_db_errors(
                'delete', 'chats',
                lambda: self.chats_repository.delete_chat_by_oid(command.chat_oid, session=session),
            )
            await _count_db_errors(
                'insert', 'outbox',
                lambda: self.outbox_repository.save_events(events, session=session),
            )
        # A deleted chat is gone for good, so unlike the message case there is no
        # generation to bump: drop the detail entry AND the version key outright.
        # (Leaving the counter behind would leak one key per deleted chat.)
        await self.cache.delete(chat_cache_key(command.chat_oid))
        await self.cache.delete(chat_version_key(command.chat_oid))
        safe_inc(cache_operations_total, operation='delete', result='ok')
        await self._mediator.publish(events)


@dataclass
class AddTelegramListenerCommand(BaseCommand):
    chat_oid: str
    telegram_chat_id: str


@dataclass
class AddTelegramListenerCommandHandler(CommandHandler[AddTelegramListenerCommand, ChatListener]):
    _mediator: EventMediator[BaseEvent, Any]
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider

    async def handle(self, command: AddTelegramListenerCommand) -> ChatListener:
        chat: Chat | None = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        chat.register_telegram_listener(telegram_chat_id=command.telegram_chat_id)
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await _count_db_errors(
                'update', 'chats',
                lambda: self.chats_repository.add_telegram_listener(
                    chat_oid=command.chat_oid,
                    telegram_chat_id=command.telegram_chat_id,
                    session=session,
                ),
            )
            await _count_db_errors(
                'insert', 'outbox',
                lambda: self.outbox_repository.save_events(events, session=session),
            )
        await self._mediator.publish(events)

        return ChatListener(oid=command.telegram_chat_id)

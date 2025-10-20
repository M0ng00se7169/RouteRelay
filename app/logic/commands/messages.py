from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    AsyncIterator,
    Optional,
    Sequence,
)

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from domain.values.messages import (
    Text,
    Title,
)
from domain.events.base import BaseEvent
from infrastructure.outbox.base import BaseOutboxRepository
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from logic.exceptions.messages import (
    ChatNotFoundException,
    ChatWithThatTitleAlreadyExistsException,
)
from logic.mediator.base import EventMediator


@asynccontextmanager
async def _maybe_transaction(
    session_provider: SessionProvider,
) -> AsyncIterator[Optional[object]]:
    session = await session_provider()
    if session is None:
        yield None
    else:
        async with session.start_transaction():
            yield session


@dataclass
class CreateChatCommand:
    title: str


@dataclass
class CreateChatCommandHandler:
    _mediator: EventMediator
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
            await self.chats_repository.add_chat(new_chat, session=session)
            await self.outbox_repository.save_events(events, session=session)
        await self._mediator.publish(events)
        return new_chat


@dataclass
class GetChatDetailQuery:
    chat_oid: str


@dataclass
class GetChatDetailQueryHandler:
    chats_repository: BaseChatsRepository

    async def handle(self, query: GetChatDetailQuery) -> Chat:
        return await self.chats_repository.get_chat_by_oid(query.chat_oid)


@dataclass
class CreateMessageCommand:
    chat_oid: str
    text: str


@dataclass
class CreateMessageCommandHandler:
    _mediator: EventMediator
    messages_repository: BaseMessagesRepository
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider

    async def handle(self, command: CreateMessageCommand) -> Message:
        chat: Chat = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        message: Message = Message(
            chat_oid=command.chat_oid,
            text=Text(command.text),
        )
        chat.add_message(message)
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await self.messages_repository.add_message(message, session=session)
            await self.outbox_repository.save_events(events, session=session)
        await self._mediator.publish(events)
        return message


@dataclass
class DeleteChatCommand:
    chat_oid: str


@dataclass
class DeleteChatCommandHandler:
    _mediator: EventMediator
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider

    async def handle(self, command: DeleteChatCommand) -> None:
        chat: Chat = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        chat.delete()
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await self.chats_repository.delete_chat_by_oid(command.chat_oid, session=session)
            await self.outbox_repository.save_events(events, session=session)
        await self._mediator.publish(events)


@dataclass
class GetAllChatsListenersQuery:
    chat_oid: str


@dataclass
class GetAllChatsListenersQueryHandler:
    chats_repository: BaseChatsRepository

    async def handle(self, query: GetAllChatsListenersQuery) -> Sequence[ChatListener]:
        return await self.chats_repository.get_all_chat_listeners(query.chat_oid)


@dataclass
class GetMessagesQuery:
    chat_oid: str
    limit: int = 50
    offset: int = 0


@dataclass
class GetMessagesQueryHandler:
    messages_repository: BaseMessagesRepository

    async def handle(self, query: GetMessagesQuery) -> Sequence[Message]:
        return await self.messages_repository.get_messages_by_chat_oid(
            chat_oid=query.chat_oid, limit=query.limit, offset=query.offset,
        )


@dataclass
class AddTelegramListenerCommand:
    chat_oid: str
    telegram_chat_id: str


@dataclass
class AddTelegramListenerCommandHandler:
    _mediator: EventMediator
    chats_repository: BaseChatsRepository
    outbox_repository: BaseOutboxRepository
    session_provider: SessionProvider

    async def handle(self, command: AddTelegramListenerCommand) -> ChatListener:
        chat: Chat = await self.chats_repository.get_chat_by_oid(command.chat_oid)

        if not chat:
            raise ChatNotFoundException(chat_oid=command.chat_oid)

        chat.register_telegram_listener(telegram_chat_id=command.telegram_chat_id)
        events = chat.pull_events()
        async with _maybe_transaction(self.session_provider) as session:
            await self.chats_repository.add_telegram_listener(
                chat_oid=command.chat_oid,
                telegram_chat_id=command.telegram_chat_id,
                session=session,
            )
            await self.outbox_repository.save_events(events, session=session)
        await self._mediator.publish(events)

        return ChatListener(oid=command.telegram_chat_id)

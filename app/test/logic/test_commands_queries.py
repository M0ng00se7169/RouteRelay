import pytest
from unittest.mock import AsyncMock, MagicMock

from domain.entities.messages import (
    Chat,
    ChatListener,
    Message,
)
from domain.values.messages import (
    Text,
    Title,
)
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.filters.messages import (
    GetAllChatsFilters,
    GetMessagesFilters,
)
from infrastructure.repositories.messages.memory import (
    MemoryChatRepository,
    MemoryMessagesRepository,
)
from logic.commands.messages import (
    AddTelegramListenerCommand,
    AddTelegramListenerCommandHandler,
    CreateChatCommand,
    CreateChatCommandHandler,
    CreateMessageCommand,
    CreateMessageCommandHandler,
    DeleteChatCommand,
    DeleteChatCommandHandler,
)
from logic.exceptions.messages import (
    ChatNotFoundException,
    ChatWithThatTitleAlreadyExistsException,
)
from logic.mediator.base import EventMediator
from logic.queries.messages import (
    GetAllChatsListenersQuery,
    GetAllChatsListenersQueryHandler,
    GetChatDetailQuery,
    GetChatDetailQueryHandler,
    GetMessagesQuery,
    GetMessagesQueryHandler,
)


class FakeMediator(EventMediator):
    def __init__(self):
        self.published = []

    def register_event(self, event):
        pass

    async def publish(self, events):
        self.published.extend(list(events))
        return []


class NoopSessionProvider(SessionProvider):
    def __init__(self):
        self.calls = 0

    async def __call__(self):
        self.calls += 1
        return None


def _chat_repo_with(title='room'):
    repo = MemoryChatRepository()
    chat = Chat.create_chat(title=Title(title))
    repo._saved_chats.append(chat)
    return repo, chat


@pytest.mark.asyncio
async def test_create_chat_success():
    mediator = FakeMediator()
    handler = CreateChatCommandHandler(
        _mediator=mediator,
        chats_repository=MemoryChatRepository(),
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    chat = await handler.handle(CreateChatCommand(title='new-room'))

    assert chat.title.as_generic_type() == 'new-room'
    assert mediator.published  # ChatCreatedEvent published


@pytest.mark.asyncio
async def test_create_chat_duplicate_raises():
    mediator = FakeMediator()
    repo = MemoryChatRepository()
    repo._saved_chats.append(Chat.create_chat(title=Title('dup')))

    handler = CreateChatCommandHandler(
        _mediator=mediator,
        chats_repository=repo,
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    with pytest.raises(ChatWithThatTitleAlreadyExistsException):
        await handler.handle(CreateChatCommand(title='dup'))


@pytest.mark.asyncio
async def test_create_message_success():
    mediator = FakeMediator()
    repo, chat = _chat_repo_with()
    messages_repo = AsyncMock()
    messages_repo.add_message = AsyncMock()
    outbox = MemoryOutboxRepository()

    handler = CreateMessageCommandHandler(
        _mediator=mediator,
        messages_repository=messages_repo,
        chats_repository=repo,
        outbox_repository=outbox,
        session_provider=NoopSessionProvider(),
    )

    message = await handler.handle(CreateMessageCommand(chat_oid=chat.oid, text='hello'))

    assert message.text.as_generic_type() == 'hello'
    messages_repo.add_message.assert_awaited_once()
    assert mediator.published  # NewMessageReceivedEvent published


@pytest.mark.asyncio
async def test_create_message_chat_not_found_raises():
    mediator = FakeMediator()
    handler = CreateMessageCommandHandler(
        _mediator=mediator,
        messages_repository=AsyncMock(),
        chats_repository=MemoryChatRepository(),
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    with pytest.raises(ChatNotFoundException):
        await handler.handle(CreateMessageCommand(chat_oid='missing', text='x'))


@pytest.mark.asyncio
async def test_delete_chat_success():
    mediator = FakeMediator()
    repo, chat = _chat_repo_with()

    handler = DeleteChatCommandHandler(
        _mediator=mediator,
        chats_repository=repo,
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    await handler.handle(DeleteChatCommand(chat_oid=chat.oid))

    assert await repo.get_chat_by_oid(chat.oid) is None
    assert mediator.published  # ChatDeletedEvent published


@pytest.mark.asyncio
async def test_delete_chat_not_found_raises():
    mediator = FakeMediator()
    handler = DeleteChatCommandHandler(
        _mediator=mediator,
        chats_repository=MemoryChatRepository(),
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    with pytest.raises(ChatNotFoundException):
        await handler.handle(DeleteChatCommand(chat_oid='ghost'))


@pytest.mark.asyncio
async def test_add_telegram_listener_success():
    mediator = FakeMediator()
    repo, chat = _chat_repo_with()

    handler = AddTelegramListenerCommandHandler(
        _mediator=mediator,
        chats_repository=repo,
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    await handler.handle(AddTelegramListenerCommand(chat_oid=chat.oid, telegram_chat_id='tg-1'))

    listeners = await repo.get_all_chat_listeners(chat.oid)
    assert ChatListener(oid='tg-1') in set(listeners)
    assert mediator.published  # ChatTelegramListenerAddedEvent published


@pytest.mark.asyncio
async def test_add_telegram_listener_chat_not_found_raises():
    mediator = FakeMediator()
    handler = AddTelegramListenerCommandHandler(
        _mediator=mediator,
        chats_repository=MemoryChatRepository(),
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    with pytest.raises(ChatNotFoundException):
        await handler.handle(AddTelegramListenerCommand(chat_oid='ghost', telegram_chat_id='tg-1'))


@pytest.mark.asyncio
async def test_get_chat_detail_query():
    repo, chat = _chat_repo_with()
    handler = GetChatDetailQueryHandler(
        chats_repository=repo, messages_repository=MemoryMessagesRepository(),
    )

    fetched = await handler.handle(GetChatDetailQuery(chat_oid=chat.oid))

    assert fetched.oid == chat.oid


@pytest.mark.asyncio
async def test_get_all_chats_listeners_query():
    repo, chat = _chat_repo_with()
    await repo.add_telegram_listener(chat.oid, 'tg-9')
    handler = GetAllChatsListenersQueryHandler(chats_repository=repo)

    listeners = await handler.handle(GetAllChatsListenersQuery(chat_oid=chat.oid))

    assert {l.oid for l in listeners} == {'tg-9'}


@pytest.mark.asyncio
async def test_get_all_chats_listeners_query_unknown_chat_raises():
    handler = GetAllChatsListenersQueryHandler(chats_repository=MemoryChatRepository())

    with pytest.raises(ChatNotFoundException):
        await handler.handle(GetAllChatsListenersQuery(chat_oid='ghost'))


@pytest.mark.asyncio
async def test_get_messages_query():
    messages_repo = AsyncMock()
    messages_repo.get_messages = AsyncMock(return_value=([], 0))
    handler = GetMessagesQueryHandler(messages_repository=messages_repo)

    await handler.handle(
        GetMessagesQuery(chat_oid='c1', filters=GetMessagesFilters(limit=10, offset=0)),
    )

    messages_repo.get_messages.assert_awaited_once_with(
        chat_oid='c1', filters=GetMessagesFilters(limit=10, offset=0),
    )


@pytest.mark.asyncio
async def test_maybe_transaction_starts_transaction_when_session_returned():
    class FakeSession:
        def start_transaction(self):
            return self

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    async def provider():
        return FakeSession()

    from logic.commands.messages import _maybe_transaction

    async with _maybe_transaction(provider) as session:
        assert session is not None


@pytest.mark.asyncio
async def test_maybe_transaction_yields_none_when_provider_returns_none():
    from logic.commands.messages import _maybe_transaction

    async with _maybe_transaction(NoopSessionProvider()) as session:
        assert session is None


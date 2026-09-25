"""DB failure counters (ADR-0006, Chunk 5.2).

``db_operation_errors_total`` counts exceptions raised by persistence calls
inside the command/query handlers, then re-raises. These tests follow the
baseline-delta house pattern (the prometheus registry is global across tests);
labels are dynamic (``operation.{ExceptionClassName}``), so assertions use
``get_sample_value`` with the full label set and treat ``None`` as 0.
"""

from dataclasses import dataclass

import pytest
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.messages.memory import (
    MemoryChatRepository,
    MemoryMessagesRepository,
)
from prometheus_client import REGISTRY

from domain.entities.messages import Chat
from domain.values.messages import Title
from logic.commands.messages import (
    CreateChatCommand,
    CreateChatCommandHandler,
    DeleteChatCommand,
    DeleteChatCommandHandler,
)
from logic.exceptions.messages import ChatNotFoundException
from logic.mediator.base import EventMediator
from logic.queries.messages import (
    GetChatDetailQuery,
    GetChatDetailQueryHandler,
)


def _db_error(operation: str, collection: str, exception: str) -> float:
    sample = REGISTRY.get_sample_value(
        'db_operation_errors_total',
        {
            'operation': f'{operation}.{exception}',
            'collection': collection,
            'exception': exception,
        },
    )
    return sample if sample is not None else 0.0


@dataclass
class FakeMediator(EventMediator):
    def __init__(self):
        self.published = []

    def register_event(self, event, event_handlers):
        pass

    async def publish(self, events):
        self.published.extend(list(events))
        return []


class NoopSessionProvider(SessionProvider):
    async def __call__(self):
        return None


class FailingChatsRepository(MemoryChatRepository):
    def __init__(self, failing_method: str):
        super().__init__()
        self.failing_method = failing_method

    def _fail(self):
        raise RuntimeError('mongo is down')

    async def add_chat(self, chat, session=None):
        self._fail()

    async def delete_chat_by_oid(self, chat_oid: str, session=None):
        self._fail()

    async def get_chat_by_oid(self, oid: str):
        if self.failing_method == 'get_chat_by_oid':
            self._fail()
        return await super().get_chat_by_oid(oid)


class FailingOutboxRepository(MemoryOutboxRepository):
    async def save_events(self, events, session=None) -> None:
        raise RuntimeError('outbox write failed')


def _chat_repo_with(title='room'):
    repo = MemoryChatRepository()
    repo._saved_chats.append(Chat.create_chat(title=Title(title)))
    return repo


@pytest.mark.asyncio
async def test_create_chat_counts_insert_failure_and_reraises():
    handler = CreateChatCommandHandler(
        _mediator=FakeMediator(),
        chats_repository=FailingChatsRepository('add_chat'),
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    before = _db_error('insert', 'chats', 'RuntimeError')

    with pytest.raises(RuntimeError, match='mongo is down'):
        await handler.handle(CreateChatCommand(title='new-room'))

    assert _db_error('insert', 'chats', 'RuntimeError') - before == 1


@pytest.mark.asyncio
async def test_outbox_write_failure_is_counted():
    mediator = FakeMediator()
    handler = CreateChatCommandHandler(
        _mediator=mediator,
        chats_repository=MemoryChatRepository(),
        outbox_repository=FailingOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    before = _db_error('insert', 'outbox', 'RuntimeError')

    with pytest.raises(RuntimeError, match='outbox write failed'):
        await handler.handle(CreateChatCommand(title='new-room'))

    assert _db_error('insert', 'outbox', 'RuntimeError') - before == 1
    # The failure happens before mediator.publish — no events may be published.
    assert mediator.published == []


@pytest.mark.asyncio
async def test_delete_chat_counts_delete_failure_and_reraises():
    repo = FailingChatsRepository('delete_chat_by_oid')
    repo._saved_chats.append(Chat.create_chat(title=Title('room')))
    chat = repo._saved_chats[0]
    handler = DeleteChatCommandHandler(
        _mediator=FakeMediator(),
        chats_repository=repo,
        outbox_repository=MemoryOutboxRepository(),
        session_provider=NoopSessionProvider(),
    )

    before = _db_error('delete', 'chats', 'RuntimeError')

    with pytest.raises(RuntimeError, match='mongo is down'):
        await handler.handle(DeleteChatCommand(chat_oid=chat.oid))

    assert _db_error('delete', 'chats', 'RuntimeError') - before == 1


@pytest.mark.asyncio
async def test_query_counts_failure_and_reraises():
    handler = GetChatDetailQueryHandler(
        chats_repository=FailingChatsRepository('get_chat_by_oid'),
        messages_repository=MemoryMessagesRepository(),
    )

    before = _db_error('query', 'chats', 'RuntimeError')

    with pytest.raises(RuntimeError, match='mongo is down'):
        await handler.handle(GetChatDetailQuery(chat_oid='c1'))

    assert _db_error('query', 'chats', 'RuntimeError') - before == 1


@pytest.mark.asyncio
async def test_domain_exception_is_not_counted_as_db_failure():
    # ChatNotFoundException is raised by the handler after a *successful*
    # repository call (chat simply absent) — it must not count as a DB error.
    handler = GetChatDetailQueryHandler(
        chats_repository=MemoryChatRepository(),
        messages_repository=MemoryMessagesRepository(),
    )

    before = _db_error('query', 'chats', 'ChatNotFoundException')

    with pytest.raises(ChatNotFoundException):
        await handler.handle(GetChatDetailQuery(chat_oid='missing'))

    assert _db_error('query', 'chats', 'ChatNotFoundException') == before

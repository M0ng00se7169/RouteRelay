"""Write-path cache invalidation (ADR-0008, Chunk 3).

The contract being pinned is *read-after-write freshness*: a message posted
now must never be served from a page cached before the post. That is what the
version bump buys, and it is only true if the invalidation happens on the same
path as the write.

The tests go through the real command handlers + cached repository proxies (the
production order: ``CircuitBreaker(Cached(Mongo))`` reduced to
``Cached(Memory)``), so the wiring is exercised rather than assumed.
"""

from collections.abc import Iterable
from typing import Any

import pytest

from domain.entities.messages import (
	Chat,
	Message,
)
from domain.events.base import BaseEvent
from domain.values.messages import (
	Text,
	Title,
)
from infrastructure.cache.cached import (
	CachedChatsRepository,
	CachedMessagesRepository,
)
from infrastructure.cache.keys import (
	chat_cache_key,
	chat_version_key,
)
from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.filters.messages import GetMessagesFilters
from infrastructure.repositories.messages.base import (
	BaseChatsRepository,
	BaseMessagesRepository,
)
from infrastructure.repositories.messages.memory import (
	MemoryChatRepository,
	MemoryMessagesRepository,
)
from logic.commands.messages import (
	CreateMessageCommand,
	CreateMessageCommandHandler,
	DeleteChatCommand,
	DeleteChatCommandHandler,
)
from logic.events.base import EventHandler
from logic.mediator.event import EventMediator
from logic.queries.messages import (
	GetChatDetailQuery,
	GetChatDetailQueryHandler,
	GetMessagesQuery,
	GetMessagesQueryHandler,
)


class FakeMediator(EventMediator[BaseEvent, Any]):
	def __init__(self) -> None:
		self.published: list[BaseEvent] = []

	def register_event(
		self,
		event: type[BaseEvent],
		event_handlers: Iterable[EventHandler[BaseEvent, Any]] | None = None,
	) -> None:
		pass

	async def publish(self, events: Iterable[BaseEvent]) -> list[Any]:
		self.published.extend(list(events))
		return []


class NoopSessionProvider(SessionProvider):
	async def __call__(self) -> None:
		return None


class FakeClock:
	def __init__(self) -> None:
		self.now = 0.0

	def __call__(self) -> float:
		return self.now

	def advance(self, seconds: float) -> None:
		self.now += seconds


def _chat(oid: str) -> Chat:
	chat = Chat(oid=oid, title=Title('room'))
	return chat


@pytest.fixture
def clock() -> FakeClock:
	return FakeClock()


@pytest.fixture
def cache(clock: FakeClock) -> MemoryCacheClient:
	return MemoryCacheClient(clock=clock)


@pytest.fixture
def chats() -> MemoryChatRepository:
	repo = MemoryChatRepository()
	repo._saved_chats.append(_chat('chat-1'))
	return repo


@pytest.fixture
def messages() -> MemoryMessagesRepository:
	return MemoryMessagesRepository()


def _cached_chats(inner: BaseChatsRepository, cache: MemoryCacheClient) -> BaseChatsRepository:
	return CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)


def _cached_messages(
	inner: BaseMessagesRepository,
	cache: MemoryCacheClient,
) -> BaseMessagesRepository:
	return CachedMessagesRepository(inner=inner, cache=cache, ttl_seconds=60)


async def _post_message(
	chats: BaseChatsRepository,
	messages: BaseMessagesRepository,
	cache: MemoryCacheClient,
	chat_oid: str,
	text: str,
) -> None:
	handler = CreateMessageCommandHandler(
		_mediator=FakeMediator(),
		messages_repository=messages,
		chats_repository=chats,
		outbox_repository=MemoryOutboxRepository(),
		session_provider=NoopSessionProvider(),
		cache=cache,
	)
	await handler.handle(CreateMessageCommand(chat_oid=chat_oid, text=text))


# --- read-after-write --------------------------------------------------------


@pytest.mark.asyncio
async def test_new_message_invalidates_the_cached_page(
	chats: MemoryChatRepository,
	messages: MemoryMessagesRepository,
	cache: MemoryCacheClient,
) -> None:
	cached_messages = _cached_messages(messages, cache)
	cached_chats = _cached_chats(chats, cache)
	filters = GetMessagesFilters(limit=10, offset=0)

	# Warm the cache: one message, one cached page.
	await _post_message(cached_chats, cached_messages, cache, 'chat-1', 'first')
	before = await GetMessagesQueryHandler(
		messages_repository=cached_messages,
	).handle(GetMessagesQuery(chat_oid='chat-1', filters=filters))
	assert [m.text.as_generic_type() for m in before[0]] == ['first']

	# Post again. Without invalidation the second read would return the page
	# cached above, i.e. 'first' only.
	await _post_message(cached_chats, cached_messages, cache, 'chat-1', 'second')
	after = await GetMessagesQueryHandler(
		messages_repository=cached_messages,
	).handle(GetMessagesQuery(chat_oid='chat-1', filters=filters))

	assert {m.text.as_generic_type() for m in after[0]} == {'first', 'second'}
	assert after[1] == 2


@pytest.mark.asyncio
async def test_post_bumps_the_chat_generation(
	chats: MemoryChatRepository,
	messages: MemoryMessagesRepository,
	cache: MemoryCacheClient,
) -> None:
	cached_messages = _cached_messages(messages, cache)
	cached_chats = _cached_chats(chats, cache)

	await _post_message(cached_chats, cached_messages, cache, 'chat-1', 'first')
	generation = await cache.get(chat_version_key('chat-1'))

	assert generation is not None
	assert int(generation) == 1


@pytest.mark.asyncio
async def test_stale_page_dies_by_ttl_even_without_the_bump(
	chats: MemoryChatRepository,
	messages: MemoryMessagesRepository,
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	# The safety net: if the invalidation is ever lost (Valkey blip, a write path
	# that forgets), staleness is bounded by the TTL, never unbounded.
	cached_messages = _cached_messages(messages, cache)
	filters = GetMessagesFilters(limit=10, offset=0)

	await messages.add_message(_message('chat-1', 'first'))
	handler = GetMessagesQueryHandler(messages_repository=cached_messages)
	await handler.handle(GetMessagesQuery(chat_oid='chat-1', filters=filters))
	# Past the worst case of the +/-10% jitter on the 60s read TTL.
	clock.advance(120)

	# Re-post WITHOUT bumping the version (simulating a lost invalidation).
	await messages.add_message(_message('chat-1', 'second'))
	after = await handler.handle(GetMessagesQuery(chat_oid='chat-1', filters=filters))

	assert {m.text.as_generic_type() for m in after[0]} == {'first', 'second'}


@pytest.mark.asyncio
async def test_invalidated_chat_detail_404s_after_delete(
	chats: MemoryChatRepository,
	cache: MemoryCacheClient,
) -> None:
	cached_chats = _cached_chats(chats, cache)
	detail = GetChatDetailQueryHandler(
		chats_repository=cached_chats,
		messages_repository=MemoryMessagesRepository(),
	)

	# Warm the cache for a chat that is about to be deleted.
	chat = await detail.handle(GetChatDetailQuery(chat_oid='chat-1'))
	assert chat.oid == 'chat-1'
	assert await cache.get(chat_cache_key('chat-1')) is not None

	await DeleteChatCommandHandler(
		_mediator=FakeMediator(),
		chats_repository=cached_chats,
		outbox_repository=MemoryOutboxRepository(),
		session_provider=NoopSessionProvider(),
		cache=cache,
	).handle(DeleteChatCommand(chat_oid='chat-1'))

	# Both keys are gone, so a deleted chat cannot be resurrected from cache.
	assert await cache.get(chat_cache_key('chat-1')) is None
	assert await cache.get(chat_version_key('chat-1')) is None
	assert await cached_chats.get_chat_by_oid('chat-1') is None


@pytest.mark.asyncio
async def test_delete_does_not_invalidate_other_chats(
	chats: MemoryChatRepository,
	cache: MemoryCacheClient,
) -> None:
	cached_chats = _cached_chats(chats, cache)
	chats._saved_chats.append(_chat('chat-2'))
	await cached_chats.get_chat_by_oid('chat-1')
	await cached_chats.get_chat_by_oid('chat-2')

	await DeleteChatCommandHandler(
		_mediator=FakeMediator(),
		chats_repository=cached_chats,
		outbox_repository=MemoryOutboxRepository(),
		session_provider=NoopSessionProvider(),
		cache=cache,
	).handle(DeleteChatCommand(chat_oid='chat-1'))

	assert await cache.get(chat_cache_key('chat-1')) is None
	assert await cache.get(chat_cache_key('chat-2')) is not None


# --- degradation -------------------------------------------------------------


@pytest.mark.asyncio
async def test_write_succeeds_even_when_the_cache_is_gone(
	chats: MemoryChatRepository,
	messages: MemoryMessagesRepository,
) -> None:
	# A cache outage must never cost a write. The in-memory client here stands
	# in for a Valkey that swallowed its own errors: the command succeeds and
	# only freshness is affected.
	class BrokenCache(MemoryCacheClient):
		async def increment(self, key: str) -> int:
			return 0

		async def delete(self, key: str) -> None:
			return None

	broken = BrokenCache()
	handler = CreateMessageCommandHandler(
		_mediator=FakeMediator(),
		messages_repository=messages,
		chats_repository=chats,
		outbox_repository=MemoryOutboxRepository(),
		session_provider=NoopSessionProvider(),
		cache=broken,
	)

	message = await handler.handle(CreateMessageCommand(chat_oid='chat-1', text='survives'))

	assert message.text.as_generic_type() == 'survives'
	assert len(messages._saved_messages) == 1


def _message(chat_oid: str, text: str) -> Message:
	return Message(chat_oid=chat_oid, text=Text(value=text))

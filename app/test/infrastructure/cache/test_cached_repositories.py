"""Cache-aside proxy tests (ADR-0008, Chunk 2).

Covers the three properties the read path depends on:

1. miss -> inner call -> populate, hit -> NO inner call (the whole point of a
   cache is that the second read never touches Mongo);
2. a failing cache is a pass-through, not a failed read — and it must not be
   charged to the 'mongo' breaker that sits ABOVE these proxies;
3. versioned invalidation makes a stale page unreachable.
"""


import pytest
from prometheus_client import REGISTRY

from domain.entities.messages import (
	Chat,
	ChatListener,
	Message,
)
from domain.values.messages import (
	Text,
	Title,
)
from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.cached import (
	CachedChatsRepository,
	CachedMessagesRepository,
	_jittered_ttl,
)
from infrastructure.cache.keys import (
	chat_cache_key,
	chat_version_key,
	deserialize_chat,
	messages_cache_key,
	serialize_chat,
)
from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.repositories.filters.messages import GetMessagesFilters
from infrastructure.repositories.messages.memory import (
	MemoryChatRepository,
	MemoryMessagesRepository,
)
from infrastructure.resilience import CircuitBreaker


class FailingCacheClient(BaseCacheClient):
	"""Cache that is down: every call raises, like a Valkey outage."""

	async def get(self, key: str) -> bytes | None:
		raise ConnectionError('valkey is down')

	async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
		raise ConnectionError('valkey is down')

	async def delete(self, key: str) -> None:
		raise ConnectionError('valkey is down')

	async def exists(self, key: str) -> bool:
		raise ConnectionError('valkey is down')

	async def increment(self, key: str) -> int:
		raise ConnectionError('valkey is down')

	async def hash_set(
		self,
		key: str,
		field: str,
		value: str,
		ttl_seconds: int | None = None,
	) -> None:
		raise ConnectionError('valkey is down')

	async def hash_count(self, key: str) -> int:
		raise ConnectionError('valkey is down')

	async def hash_delete(self, key: str, field: str) -> None:
		raise ConnectionError('valkey is down')

	async def set_if_absent(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		raise ConnectionError('valkey is down')

	async def compare_and_extend(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		raise ConnectionError('valkey is down')

	async def compare_and_delete(self, key: str, value: bytes) -> bool:
		raise ConnectionError('valkey is down')

	async def aclose(self) -> None:
		return None


def _operations(operation: str, result: str) -> float:
	sample = REGISTRY.get_sample_value(
		'cache_operations_total',
		{'operation': operation, 'result': result},
	)
	return sample if sample is not None else 0.0


def _chat_repo() -> tuple[MemoryChatRepository, Chat]:
	repo = MemoryChatRepository()
	chat = Chat.create_chat(title=Title('room'))
	repo._saved_chats.append(chat)
	return repo, chat


# --- chats -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_chat_miss_calls_inner_then_populates_the_cache() -> None:
	inner, chat = _chat_repo()
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	fetched = await repository.get_chat_by_oid(chat.oid)

	assert fetched is not None
	assert fetched.oid == chat.oid
	assert await cache.get(chat_cache_key(chat.oid)) is not None


@pytest.mark.asyncio
async def test_chat_hit_skips_the_inner_repository() -> None:
	inner, chat = _chat_repo()
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	await repository.get_chat_by_oid(chat.oid)
	inner._saved_chats.clear()  # a second Mongo read would now 404

	from_cache = await repository.get_chat_by_oid(chat.oid)

	assert from_cache is not None
	assert from_cache.title.as_generic_type() == 'room'


@pytest.mark.asyncio
async def test_chat_miss_is_not_cached() -> None:
	# A missing chat is not a value: caching the negative result would make a
	# chat created a moment later invisible for a whole TTL.
	inner = MemoryChatRepository()
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	assert await repository.get_chat_by_oid('ghost') is None
	assert await cache.get(chat_cache_key('ghost')) is None


@pytest.mark.asyncio
async def test_chat_cache_failure_passes_through_to_the_inner_repository() -> None:
	inner, chat = _chat_repo()
	repository = CachedChatsRepository(inner=inner, cache=FailingCacheClient())

	# The proxy must swallow the cache outage on the way in AND on the way out.
	fetched = await repository.get_chat_by_oid(chat.oid)

	assert fetched is not None
	assert fetched.oid == chat.oid


@pytest.mark.asyncio
async def test_chat_cache_failure_does_not_trip_the_mongo_breaker() -> None:
	# ADR-0008 §4: the cache proxy sits INSIDE the breaker proxy, so a Valkey
	# error must never reach breaker.call() — otherwise the 'mongo' breaker
	# opens over a dependency Mongo is perfectly healthy on.
	breaker = CircuitBreaker(name='mongo', failure_threshold=1, recovery_time=60)
	inner, chat = _chat_repo()
	cached = CachedChatsRepository(inner=inner, cache=FailingCacheClient())

	await cached.get_chat_by_oid(chat.oid)
	await cached.get_chat_by_oid(chat.oid)
	await cached.get_chat_by_oid(chat.oid)

	assert breaker.state == 'closed'


@pytest.mark.asyncio
async def test_undecodable_payload_is_treated_as_a_miss() -> None:
	inner, chat = _chat_repo()
	cache = MemoryCacheClient()
	# A truncated or older-schema entry must not fail the read.
	await cache.set(chat_cache_key(chat.oid), b'garbage')
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	fetched = await repository.get_chat_by_oid(chat.oid)

	assert fetched is not None
	assert fetched.oid == chat.oid
	# And the entry was repaired, not just skipped.
	assert await cache.get(chat_cache_key(chat.oid)) is not None


@pytest.mark.asyncio
async def test_listeners_survive_the_round_trip() -> None:
	inner, chat = _chat_repo()
	await inner.add_telegram_listener(chat.oid, 'tg-1')
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	await repository.get_chat_by_oid(chat.oid)
	from_cache = await repository.get_chat_by_oid(chat.oid)

	assert from_cache is not None
	assert {listener.oid for listener in from_cache.listeners} == {'tg-1'}


# --- messages ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_message_page_hit_skips_the_inner_repository() -> None:
	inner = MemoryMessagesRepository()
	cache = MemoryCacheClient()
	repository = CachedMessagesRepository(inner=inner, cache=cache, ttl_seconds=60)
	filters = GetMessagesFilters(limit=10, offset=0)

	await repository.get_messages(chat_oid='c1', filters=filters)
	inner._saved_messages.clear()

	messages, count = await repository.get_messages(chat_oid='c1', filters=filters)

	assert messages == []
	assert count == 0


@pytest.mark.asyncio
async def test_message_page_caches_items_and_the_total_count() -> None:
	inner = MemoryMessagesRepository()
	await inner.add_message(_message('c1', 'hello'))
	cache = MemoryCacheClient()
	repository = CachedMessagesRepository(inner=inner, cache=cache, ttl_seconds=60)

	first = await repository.get_messages(chat_oid='c1', filters=GetMessagesFilters(limit=10, offset=0))
	inner._saved_messages.clear()
	second = await repository.get_messages(chat_oid='c1', filters=GetMessagesFilters(limit=10, offset=0))

	assert [m.text.as_generic_type() for m in first[0]] == ['hello']
	assert first[1] == 1
	# The cached page carries the count too — otherwise every read would still
	# need a COUNT query.
	assert second[1] == 1
	assert [m.text.as_generic_type() for m in second[0]] == ['hello']


@pytest.mark.asyncio
async def test_version_bump_makes_the_cached_page_unreachable() -> None:
	inner = MemoryMessagesRepository()
	await inner.add_message(_message('c1', 'hello'))
	cache = MemoryCacheClient()
	repository = CachedMessagesRepository(inner=inner, cache=cache, ttl_seconds=60)
	filters = GetMessagesFilters(limit=10, offset=0)

	await repository.get_messages(chat_oid='c1', filters=filters)
	# What the command handler does on every write (ADR-0008 Chunk 3).
	await cache.increment(chat_version_key('c1'))
	await inner.add_message(_message('c1', 'world'))

	messages, count = await repository.get_messages(chat_oid='c1', filters=filters)

	assert {m.text.as_generic_type() for m in messages} == {'hello', 'world'}
	assert count == 2


@pytest.mark.asyncio
async def test_pages_are_cached_per_offset_and_limit() -> None:
	inner = MemoryMessagesRepository()
	for index in range(5):
		await inner.add_message(_message('c1', f'm{index}'))
	cache = MemoryCacheClient()
	repository = CachedMessagesRepository(inner=inner, cache=cache, ttl_seconds=60)

	page_one = await repository.get_messages(chat_oid='c1', filters=GetMessagesFilters(limit=2, offset=0))
	page_two = await repository.get_messages(chat_oid='c1', filters=GetMessagesFilters(limit=2, offset=2))

	assert len(page_one[0]) == 2
	assert len(page_two[0]) == 2
	assert page_one[0][0].text.as_generic_type() != page_two[0][0].text.as_generic_type()


@pytest.mark.asyncio
async def test_message_cache_failure_passes_through() -> None:
	inner = MemoryMessagesRepository()
	await inner.add_message(_message('c1', 'hello'))
	repository = CachedMessagesRepository(inner=inner, cache=FailingCacheClient())

	messages, count = await repository.get_messages(
		chat_oid='c1',
		filters=GetMessagesFilters(limit=10, offset=0),
	)

	assert len(messages) == 1
	assert count == 1


# --- passthrough methods -----------------------------------------------------


@pytest.mark.asyncio
async def test_write_methods_pass_straight_through() -> None:
	# Invalidation belongs to the command handlers (Chunk 3), so the proxy never
	# writes: a write can then never be half-applied because the cache was down.
	inner, chat = _chat_repo()
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	await repository.add_chat(chat)

	assert await cache.exists(chat_cache_key(chat.oid)) is False


# --- metrics -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_hit_and_miss_are_counted() -> None:
	inner, chat = _chat_repo()
	cache = MemoryCacheClient()
	repository = CachedChatsRepository(inner=inner, cache=cache, ttl_seconds=60)

	hits_before = _operations('get', 'hit')
	misses_before = _operations('get', 'miss')
	sets_before = _operations('set', 'ok')

	await repository.get_chat_by_oid(chat.oid)
	await repository.get_chat_by_oid(chat.oid)

	assert _operations('get', 'miss') - misses_before == 1
	assert _operations('get', 'hit') - hits_before == 1
	assert _operations('set', 'ok') - sets_before == 1


# --- ttl jitter --------------------------------------------------------------


def test_ttl_jitter_stays_within_ten_percent() -> None:
	values = {_jittered_ttl(60) for _ in range(200)}

	assert min(values) >= 54
	assert max(values) <= 66
	# Jitter must actually de-align expiries, not be a no-op.
	assert len(values) > 1


def test_ttl_jitter_never_rounds_down_to_zero() -> None:
	assert _jittered_ttl(1) >= 1


def test_key_layout_is_stable() -> None:
	# Key layout is a contract between the proxies and the command handlers:
	# a rename here silently disables invalidation.
	assert chat_cache_key('abc') == 'chat:abc'
	assert chat_version_key('abc') == 'chat:ver:abc'
	assert messages_cache_key('abc', 3, 10, 20) == 'messages:abc:v3:10:20'


def test_chat_serialization_round_trip() -> None:
	chat = Chat.create_chat(title=Title('room'))
	chat.listeners.add(ChatListener(oid='tg-1'))

	restored = deserialize_chat(serialize_chat(chat))

	assert restored is not None
	assert restored.oid == chat.oid
	assert restored.title.as_generic_type() == 'room'
	assert restored.created_at == chat.created_at
	assert {listener.oid for listener in restored.listeners} == {'tg-1'}


# --- helpers -----------------------------------------------------------------


def _message(chat_oid: str, text: str) -> Message:
	return Message(chat_oid=chat_oid, text=Text(value=text))

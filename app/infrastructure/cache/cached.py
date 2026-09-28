"""Cache-aside repository proxies (ADR-0008, Chunk 2).

Same decorator pattern as the circuit-breaker proxies in
``infrastructure/resilience.py``: an explicit proxy class per repository that
wraps the "real" one, so every cached call site stays greppable and the inner
repository is typed against its ABC (Mongo, memory, or another proxy all fit).

Wiring order, and why it is that way (ADR-0008 §4)::

    CircuitBreaker(Cached(Mongo))
      ^ guards          ^ degrades    ^ the source of truth

The cache proxies sit INSIDE the breaker proxies: the breaker must keep
measuring Mongo's health, so a Valkey error has to be swallowed down here and
never reach ``breaker.call()``. Getting the order the other way round would
charge every cache blip to the 'mongo' breaker and could open it over a
dependency Mongo is perfectly happy with.

Reads are cache-aside (check cache → miss → read Mongo → populate), writes are
passed straight through: invalidation is the command handlers' job (Chunk 3),
not the proxy's, so a write can never be half-applied "because the cache was
down".
"""

import logging
from dataclasses import dataclass
from random import uniform

from domain.entities.messages import (
	Chat,
	ChatListener,
	Message,
)
from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.keys import (
	chat_cache_key,
	chat_version_key,
	deserialize_chat,
	deserialize_messages,
	messages_cache_key,
	serialize_chat,
	serialize_messages,
)
from infrastructure.metrics import (
	cache_errors_total,
	cache_operations_total,
	safe_inc,
)
from infrastructure.repositories.filters.messages import (
	GetAllChatsFilters,
	GetMessagesFilters,
)
from infrastructure.repositories.messages.base import (
	BaseChatsRepository,
	BaseMessagesRepository,
	SessionHint,
)

logger = logging.getLogger(__name__)

# Read-path TTL is jittered by +/-10% so keys written by the same deploy for the
# same chat do not all expire on the same second and stampede Mongo together.
TTL_JITTER_RATIO = 0.1


def _jittered_ttl(ttl_seconds: int) -> int:
	"""Spread a TTL over +/-10% (ADR-0008 §2.2)."""
	spread = ttl_seconds * TTL_JITTER_RATIO
	return max(1, int(ttl_seconds + uniform(-spread, spread)))


def _count(operation: str, result: str) -> None:
	safe_inc(cache_operations_total, operation=operation, result=result)


async def _safe_get(cache: BaseCacheClient, key: str) -> bytes | None:
	"""Cache read that degrades to a miss, whatever the client does.

	``ValkeyCacheClient`` already converts its own failures into a miss, so this
	is normally a passthrough. It exists because the alternative is worse: a
	cache implementation (or a future Valkey client option) that raised would
	turn a cache outage into a failed request, and — since these proxies sit
	INSIDE the 'mongo' breaker — would also charge Valkey's failure to Mongo.
	"""
	try:
		return await cache.get(key)
	except Exception:
		logger.warning('Cache read of %s failed; reading the source of truth', key, exc_info=True)
		safe_inc(cache_errors_total, operation='get')
		_count('get', 'error')
		return None


async def _safe_set(
	cache: BaseCacheClient,
	key: str,
	payload: bytes,
	ttl_seconds: int,
) -> None:
	"""Cache write that is dropped on failure (a read-only degradation)."""
	try:
		await cache.set(key, payload, ttl_seconds=ttl_seconds)
	except Exception:
		logger.warning('Cache write of %s failed; the read path will miss', key, exc_info=True)
		safe_inc(cache_errors_total, operation='set')
		_count('set', 'error')
		return
	_count('set', 'ok')


@dataclass
class CachedChatsRepository(BaseChatsRepository):
	"""Chats repository with a cached ``get_chat_by_oid``."""

	inner: BaseChatsRepository
	cache: BaseCacheClient
	ttl_seconds: int = 60

	# The list endpoint is user-scoped and paginated with arbitrary filters, so
	# there is no stable key for it; it stays uncached (the ADR scopes caching to
	# the detail read and the messages pages).

	async def get_chat_by_oid(self, oid: str) -> Chat | None:
		key = chat_cache_key(oid)

		payload = await _safe_get(self.cache, key)
		if payload is not None:
			cached = deserialize_chat(payload)
			# A payload we cannot decode is treated as a miss, not an error:
			# fall through to Mongo rather than failing the read.
			if cached is not None:
				_count('get', 'hit')
				return cached

		_count('get', 'miss')
		chat = await self.inner.get_chat_by_oid(oid=oid)
		if chat is not None:
			await _safe_set(
				self.cache,
				key,
				serialize_chat(chat),
				_jittered_ttl(self.ttl_seconds),
			)
		return chat

	async def check_chat_exists_by_title(self, title: str) -> bool:
		return await self.inner.check_chat_exists_by_title(title=title)

	async def add_chat(self, chat: Chat, session: SessionHint | None = None) -> None:
		await self.inner.add_chat(chat, session=session)

	async def get_all_chats(self, filters: GetAllChatsFilters) -> tuple[list[Chat], int]:
		return await self.inner.get_all_chats(filters=filters)

	async def delete_chat_by_oid(self, chat_oid: str, session: SessionHint | None = None) -> None:
		await self.inner.delete_chat_by_oid(chat_oid, session=session)

	async def add_telegram_listener(
		self,
		chat_oid: str,
		telegram_chat_id: str,
		session: SessionHint | None = None,
	) -> None:
		await self.inner.add_telegram_listener(
			chat_oid=chat_oid,
			telegram_chat_id=telegram_chat_id,
			session=session,
		)

	async def get_all_chat_listeners(self, chat_oid: str) -> list[ChatListener]:
		listeners: list[ChatListener] = list(
			await self.inner.get_all_chat_listeners(chat_oid=chat_oid),
		)
		return listeners


@dataclass
class CachedMessagesRepository(BaseMessagesRepository):
	"""Messages repository with versioned page caching."""

	inner: BaseMessagesRepository
	cache: BaseCacheClient
	ttl_seconds: int = 60

	async def get_messages(
		self,
		chat_oid: str,
		filters: GetMessagesFilters,
	) -> tuple[list[Message], int]:
		# Read the generation first: the version is what binds a page key to a
		# point in time, so it has to be resolved before the key is built. It is
		# a tiny cached counter, and a miss there just means generation 0.
		version = await self._current_version(chat_oid)
		key = messages_cache_key(
			chat_oid=chat_oid,
			version=version,
			offset=filters.offset,
			limit=filters.limit,
		)

		payload = await _safe_get(self.cache, key)
		if payload is not None:
			page = deserialize_messages(payload)
			if page is not None:
				_count('get', 'hit')
				return page

		_count('get', 'miss')
		messages, count = await self.inner.get_messages(chat_oid=chat_oid, filters=filters)
		await _safe_set(
			self.cache,
			key,
			serialize_messages(messages, count),
			_jittered_ttl(self.ttl_seconds),
		)
		return messages, count

	async def _current_version(self, chat_oid: str) -> int:
		"""Current invalidation generation of the chat (0 when unknown)."""
		raw = await _safe_get(self.cache, chat_version_key(chat_oid))
		if raw is None:
			return 0
		try:
			return int(raw)
		except ValueError:
			# Unreadable generation -> treat as a fresh one. Worst case is a
			# miss on the next read, never a wrong page being served.
			return 0

	async def add_message(self, message: Message, session: SessionHint | None = None) -> None:
		await self.inner.add_message(message, session=session)

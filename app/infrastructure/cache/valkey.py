"""Valkey-backed cache client — the ONLY module that imports ``valkey``.

Everything else in the app talks to ``BaseCacheClient``; keeping the import
here is what lets the tests run without a Valkey server and lets a future
Redis/KeyDB swap touch exactly one file.

Two ADR-0008 decisions live in this file:

**Graceful degradation (D5).** Every operation is best-effort. A failure is
counted on ``cache_errors_total``, logged as a warning, and converted into the
value a cold cache would have produced (None / False / 0 / no-op). Reads then
fall through to Mongo and writes still succeed, so a Valkey outage costs
latency, never availability. The ``'valkey'`` circuit breaker (the third
instance in this app, alongside ``'mongo'`` and ``'kafka'``) rides on top so a
dead server does not pay a connect timeout on every single request.

**The breaker never reaches the caller.** ``CircuitOpenError`` is caught here
like any other cache failure. That matters: an open breaker must not surface as
HTTP 503 from a read endpoint — the whole point of a cache is that losing it is
a degradation, not an outage.

Connection handling follows the aiokafka lesson from PROJECT_MEMORY: the client
is created eagerly (it does no I/O until the first command) and closed
explicitly via ``aclose()`` from the app lifespan.

Typing note (the risk ADR-0008 §2.1 flagged, hit for real): valkey-py annotates
every command as ``Union[Awaitable[T], T]`` because one class serves both the
sync and the async client, and mypy refuses to await that union. So the client
is held as ``Any`` at this single boundary — the exact pattern already used for
aiokafka in message_brokers/kafka.py — and every method below re-asserts its
real return type in the small ``async def`` closures, so nothing downstream of
this file sees an ``Any``.
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from valkey.asyncio import Valkey

from infrastructure.cache.base import BaseCacheClient
from infrastructure.metrics import (
	cache_errors_total,
	safe_inc,
)
from infrastructure.resilience import (
	CircuitBreaker,
	CircuitOpenError,
)

logger = logging.getLogger(__name__)

_T = TypeVar('_T')

# Compare-and-extend (renew) and compare-and-delete (release) as Lua scripts:
# both must be a single atomic server-side operation, otherwise a lease could be
# resurrected or deleted by a process that no longer owns it.
_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


@dataclass
class ValkeyCacheClient(BaseCacheClient):
	"""Best-effort async cache client over a single Valkey instance."""

	url: str
	# Optional: the container passes the shared 'valkey' breaker; tests build
	# the client without one. None means "no breaker, just the best-effort
	# error handling".
	breaker: CircuitBreaker | None = None
	# `Any` at this one boundary: valkey-py's commands are typed
	# `Union[Awaitable[T], T]` (shared sync/async class) and mypy cannot await
	# that. See the module docstring — the same treatment aiokafka gets.
	_client: Any = None

	def _get_client(self) -> Any:
		# Lazy so a client is only materialised when a feature is actually
		# enabled (valkey.asyncio.Valkey itself performs no I/O on construction).
		if self._client is None:
			self._client = Valkey.from_url(
				self.url,
				# Short timeouts: a cache call must not outlast the request it
				# is serving, and the breaker needs failures to arrive fast.
				socket_connect_timeout=1.0,
				socket_timeout=1.0,
				health_check_interval=30,
			)
		return self._client

	async def _guard(
		self,
		operation: str,
		call: Callable[[], Awaitable[_T]],
		fallback: _T,
	) -> _T:
		"""Run one cache operation under the breaker, degrading on any failure.

		``cache_errors_total`` counts real Valkey failures (pre-breaker, per
		ADR-0008 §2.4). Breaker rejections are counted separately by the breaker
		itself on ``circuit_breaker_rejected_total{name='valkey'}``, so this
		metric stays a clean signal for "Valkey is erroring", not "Valkey is
		erroring or the breaker is already open".
		"""
		try:
			if self.breaker is not None:
				return await self.breaker.call(call)
			return await call()
		except CircuitOpenError:
			# The cache is gone until the breaker's half-open probe. Degrade
			# silently (the breaker already counted the rejection) — raising
			# here would turn a cache outage into an API outage.
			return fallback
		except Exception:
			logger.warning('Valkey %s failed; falling back to the uncached path', operation, exc_info=True)
			safe_inc(cache_errors_total, operation=operation)
			return fallback

	# --- strings (cache-aside reads) ---------------------------------------
	#
	# NOTE: every operation is wrapped in a small `async def` closure instead of
	# a lambda. That is where the declared return type is re-asserted after the
	# `Any` boundary above (bool(...) on SET NX, int(...) on the Lua results),
	# so the best-effort wrapper still sees a real Awaitable[T].

	async def get(self, key: str) -> bytes | None:
		async def read() -> bytes | None:
			value: bytes | None = await self._get_client().get(key)
			return value

		return await self._guard('get', read, None)

	async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
		async def write() -> bool:
			return bool(await self._get_client().set(key, value, ex=ttl_seconds))

		await self._guard('set', write, False)

	async def delete(self, key: str) -> None:
		async def drop() -> int:
			return int(await self._get_client().delete(key))

		await self._guard('delete', drop, 0)

	async def exists(self, key: str) -> bool:
		async def check() -> bool:
			return bool(await self._get_client().exists(key))

		return await self._guard('exists', check, False)

	# --- counters (versioned invalidation) ---------------------------------

	async def increment(self, key: str) -> int:
		async def bump() -> int:
			# Generation starts at 1, not 0: a missing key and "generation 0"
			# would otherwise produce identical page keys before the first write.
			return int(await self._get_client().incr(key))

		return await self._guard('increment', bump, 0)

	# --- hashes (chat presence) --------------------------------------------

	async def hash_set(
		self,
		key: str,
		field: str,
		value: str,
		ttl_seconds: int | None = None,
	) -> None:
		# HSET + EXPIRE as a pipeline: one round trip, and the TTL is armed on
		# every heartbeat so the key survives exactly as long as a socket keeps
		# beating.
		async def write() -> Any:
			pipe = self._get_client().pipeline(transaction=False)
			pipe.hset(key, field, value)
			if ttl_seconds is not None:
				pipe.expire(key, ttl_seconds)
			return await pipe.execute()

		await self._guard('hash_set', write, None)

	async def hash_count(self, key: str) -> int:
		async def count() -> int:
			return int(await self._get_client().hlen(key))

		return await self._guard('hash_count', count, 0)

	async def hash_delete(self, key: str, field: str) -> None:
		async def drop() -> int:
			return int(await self._get_client().hdel(key, field))

		await self._guard('hash_delete', drop, 0)

	# --- lease primitives (outbox relay leader lock) -----------------------

	async def set_if_absent(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		async def acquire() -> bool:
			# valkey-py returns None (not False) when SET ... NX finds the key
			# already there, so the result is coerced explicitly — callers, and
			# the lease's `is_held` bookkeeping, want a real bool.
			created = await self._get_client().set(key, value, nx=True, px=ttl_milliseconds)
			return bool(created)

		return await self._guard(
			'set_if_absent',
			acquire,
			# Degrade to "could not acquire": the relay skips its tick rather
			# than publishing unlocked. Duplicated outbox rows are already an
			# expected replay case (at-least-once), so skipping is the safe side.
			False,
		)

	async def compare_and_extend(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		async def renew() -> bool:
			# ARGV values are strings: the Lua bridge stringifies every argument
			# and the server parses the ttl back into an integer.
			extended = await self._get_client().eval(
				_RENEW_SCRIPT,
				1,
				key,
				value.decode(),
				str(ttl_milliseconds),
			)
			return bool(extended)

		return await self._guard('compare_and_extend', renew, False)

	async def compare_and_delete(self, key: str, value: bytes) -> bool:
		async def release() -> bool:
			deleted = await self._get_client().eval(_RELEASE_SCRIPT, 1, key, value.decode())
			return bool(deleted)

		return await self._guard('compare_and_delete', release, False)

	# --- lifecycle ----------------------------------------------------------

	async def aclose(self) -> None:
		# Idempotent and non-raising: shutdown must not fail because the cache
		# connection is already gone.
		client, self._client = self._client, None
		if client is None:
			return
		try:
			await client.aclose()
		except Exception:
			logger.warning('Failed to close the Valkey client cleanly', exc_info=True)

"""Valkey client tests (ADR-0008, Chunk 1).

No Valkey server: the client's whole job is to turn a *failing* or *succeeding*
store into the values the callers expect, so the store is a hand-written double
and the breaker is the real one. That keeps the most important property of this
class — graceful degradation — covered without a container.

The real valkey-py client (`Valkey.from_url`, pipelines, the two Lua scripts) is
verified by the live smoke check in docs/adr/0008 §3, not by pytest.
"""

from typing import Any

import pytest
from prometheus_client import REGISTRY

from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.valkey import ValkeyCacheClient
from infrastructure.resilience import (
	CircuitBreaker,
	CircuitOpenError,
)


class FakePipeline:
	def __init__(self, store: 'FakeValkey') -> None:
		self._store = store

	def hset(self, name: str, field: str, value: str) -> None:
		self._store.hashes.setdefault(name, {})[field] = value

	def expire(self, name: str, seconds: int) -> None:
		self._store.expiries[name] = seconds

	async def execute(self) -> list[Any]:
		return []


class FakeValkey:
	"""Minimal stand-in for valkey.asyncio.Valkey."""

	def __init__(self) -> None:
		self.strings: dict[str, bytes] = {}
		self.hashes: dict[str, dict[str, str]] = {}
		self.expiries: dict[str, int] = {}
		self.closed = False
		self.calls: list[tuple[str, tuple[Any, ...]]] = []
		# Set to make every command raise (simulates a down server).
		self.failing = False

	async def get(self, name: str) -> bytes | None:
		self._record('get', name)
		return self.strings.get(name)

	async def set(self, name: str, value: bytes, **kwargs: Any) -> Any:
		self._record('set', name, value, kwargs)
		if kwargs.get('nx') and name in self.strings:
			return None
		self.strings[name] = value
		return True

	async def delete(self, name: str) -> int:
		self._record('delete', name)
		return int(self.strings.pop(name, None) is not None)

	async def exists(self, name: str) -> int:
		self._record('exists', name)
		return int(name in self.strings)

	async def incr(self, name: str) -> int:
		self._record('incr', name)
		new_value = int(self.strings.get(name, b'0')) + 1
		self.strings[name] = str(new_value).encode()
		return new_value

	def pipeline(self, transaction: bool = False) -> FakePipeline:
		return FakePipeline(self)

	async def hlen(self, name: str) -> int:
		self._record('hlen', name)
		return len(self.hashes.get(name, {}))

	async def hdel(self, name: str, field: str) -> int:
		self._record('hdel', name, field)
		fields = self.hashes.get(name)
		if not fields:
			return 0
		return int(fields.pop(field, None) is not None)

	async def eval(self, script: str, numkeys: int, *keys_and_args: Any) -> int:
		self._record('eval', script, keys_and_args)
		key, expected = keys_and_args[0], keys_and_args[1]
		if self.strings.get(key) != expected:
			return 0
		if 'pexpire' in script:
			return 1
		self.strings.pop(key, None)
		return 1

	async def aclose(self) -> None:
		self.closed = True

	def _record(self, command: str, *args: Any) -> None:
		self.calls.append((command, args))
		if self.failing:
			raise ConnectionError('valkey is down')


def _client(
	store: FakeValkey,
	breaker: CircuitBreaker | None = None,
) -> ValkeyCacheClient:
	client = ValkeyCacheClient(url='redis://valkey:6379/0', breaker=breaker)
	# Inject the double instead of letting the client build a real one; the
	# private field is the seam the container never uses.
	client._client = store  # test double
	return client


def _errors(operation: str) -> float:
	sample = REGISTRY.get_sample_value('cache_errors_total', {'operation': operation})
	return sample if sample is not None else 0.0


@pytest.fixture
def store() -> FakeValkey:
	return FakeValkey()


@pytest.fixture
def client(store: FakeValkey) -> ValkeyCacheClient:
	return _client(store)


# --- happy path --------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_and_get_round_trip(client: ValkeyCacheClient, store: FakeValkey) -> None:
	await client.set('k', b'v', ttl_seconds=30)

	assert await client.get('k') == b'v'
	# ex= is the real SET EX argument the ttl travels as.
	assert ('set', ('k', b'v', {'ex': 30})) in store.calls


@pytest.mark.asyncio
async def test_increment_returns_the_new_generation(client: ValkeyCacheClient) -> None:
	assert await client.increment('chat:ver:c1') == 1
	assert await client.increment('chat:ver:c1') == 2


@pytest.mark.asyncio
async def test_hash_set_uses_a_pipeline_and_arms_the_ttl(
	client: ValkeyCacheClient,
	store: FakeValkey,
) -> None:
	await client.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)

	assert store.hashes['presence:c1'] == {'socket-1': '1'}
	assert store.expiries['presence:c1'] == 30


@pytest.mark.asyncio
async def test_lease_acquire_uses_set_nx_px(client: ValkeyCacheClient, store: FakeValkey) -> None:
	assert await client.set_if_absent('lock', b'holder-1', ttl_milliseconds=10_000) is True
	assert ('set', ('lock', b'holder-1', {'nx': True, 'px': 10_000})) in store.calls
	assert await client.set_if_absent('lock', b'holder-2', ttl_milliseconds=10_000) is False


# --- graceful degradation ----------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
	('operation', 'call', 'expected'),
	[
		('get', lambda c: c.get('k'), None),
		('exists', lambda c: c.exists('k'), False),
		('increment', lambda c: c.increment('v'), 0),
		('hash_count', lambda c: c.hash_count('presence:c1'), 0),
		('set_if_absent', lambda c: c.set_if_absent('lock', b'x', 1000), False),
		('compare_and_extend', lambda c: c.compare_and_extend('lock', b'x', 1000), False),
		('compare_and_delete', lambda c: c.compare_and_delete('lock', b'x'), False),
	],
)
async def test_every_operation_degrades_to_a_cold_cache(
	store: FakeValkey,
	operation: str,
	call: Any,
	expected: Any,
) -> None:
	client = _client(store)
	store.failing = True

	# Baseline-delta pattern: the prometheus registry is global across tests.
	before = _errors(operation)

	assert await call(client) == expected
	assert _errors(operation) - before == 1


@pytest.mark.asyncio
async def test_write_operations_degrade_without_raising(store: FakeValkey) -> None:
	client = _client(store)
	store.failing = True

	await client.set('k', b'v')
	await client.delete('k')
	await client.hash_set('presence:c1', 's', '1', 30)
	await client.hash_delete('presence:c1', 's')


@pytest.mark.asyncio
async def test_lease_acquire_failure_means_do_not_publish(
	store: FakeValkey,
) -> None:
	# The asymmetric fallback that matters: an unreadable lease must read as
	# "could not acquire" so the relay SKIPS, never as "free to publish".
	client = _client(store)
	store.failing = True

	assert await client.set_if_absent('lock', b'holder', 10_000) is False


@pytest.mark.asyncio
async def test_breaker_rejection_never_raises_at_the_cache_boundary() -> None:
	# A cache outage must not become an API outage: CircuitOpenError is mapped to
	# HTTP 503 app-wide, and the cache must never reach it.
	breaker = CircuitBreaker(name='valkey', failure_threshold=1, recovery_time=60)
	store = FakeValkey()
	store.failing = True
	client = _client(store, breaker=breaker)

	assert await client.get('k') is None
	# The breaker counted the failure and opened.
	assert breaker.state == 'open'

	before = _errors('get')
	assert await client.get('k') is None
	# Rejections are NOT Valkey errors — they keep the pre-breaker counter clean.
	assert _errors('get') == before


@pytest.mark.asyncio
async def test_circuit_open_error_from_the_breaker_is_swallowed() -> None:
	class AlwaysOpenBreaker(CircuitBreaker):
		async def call(self, operation: Any) -> Any:
			raise CircuitOpenError(name='valkey')

	client = _client(FakeValkey(), breaker=AlwaysOpenBreaker(name='valkey'))

	assert await client.get('k') is None
	assert await client.set_if_absent('lock', b'x', 1000) is False


@pytest.mark.asyncio
async def test_error_is_logged_as_a_warning(
	store: FakeValkey,
	caplog: pytest.LogCaptureFixture,
) -> None:
	import logging

	client = _client(store)
	store.failing = True

	with caplog.at_level(logging.WARNING, logger='infrastructure.cache.valkey'):
		await client.get('k')

	assert any('Valkey get failed' in record.getMessage() for record in caplog.records)


# --- lifecycle ---------------------------------------------------------------


@pytest.mark.asyncio
async def test_aclose_closes_the_pool_once(client: ValkeyCacheClient, store: FakeValkey) -> None:
	await client.aclose()

	assert store.closed is True


@pytest.mark.asyncio
async def test_aclose_without_a_client_is_a_noop() -> None:
	# Nothing was ever materialized (all features off) — shutdown must not fail.
	await ValkeyCacheClient(url='redis://valkey:6379/0').aclose()


@pytest.mark.asyncio
async def test_aclose_swallows_close_errors(store: FakeValkey) -> None:
	client = _client(store)

	async def exploding_aclose() -> None:
		raise RuntimeError('socket already gone')

	store.aclose = exploding_aclose  # type: ignore[method-assign]  # test double

	await client.aclose()


@pytest.mark.asyncio
async def test_client_is_created_lazily() -> None:
	client = ValkeyCacheClient(url='redis://valkey:6379/0')
	assert client._client is None

	# Touching the factory is what materializes it; construction itself must not
	# reach out to a server (the app boots with the flags off, and even with them
	# on, startup must not depend on Valkey being up).
	factory = client._get_client
	assert callable(factory)


def test_client_implements_the_cache_contract(client: ValkeyCacheClient) -> None:
	assert isinstance(client, BaseCacheClient)

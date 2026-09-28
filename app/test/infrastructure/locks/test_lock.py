"""Distributed lock tests (ADR-0008, Chunk 5).

The lease is the piece where a bug is silent (two relays publishing, or nobody
publishing), so the three properties that make it safe are pinned explicitly:

1. mutual exclusion while a holder is alive,
2. expiry so a dead leader does not block the others forever,
3. owner-checked renew and release, so a process that came back from the dead
   cannot extend or delete somebody else's lease.
"""

import pytest

from infrastructure.cache.keys import relay_lock_key
from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.locks.base import BaseDistributedLock
from infrastructure.locks.memory import MemoryLeaseLock
from infrastructure.locks.valkey import (
	MIN_RENEW_INTERVAL,
	ValkeyLeaseLock,
)


class FakeClock:
	def __init__(self) -> None:
		self.now = 0.0

	def __call__(self) -> float:
		return self.now

	def advance(self, seconds: float) -> None:
		self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
	return FakeClock()


@pytest.fixture
def cache(clock: FakeClock) -> MemoryCacheClient:
	return MemoryCacheClient(clock=clock)


def _lock(cache: MemoryCacheClient, holder_id: str) -> MemoryLeaseLock:
	return MemoryLeaseLock(
		cache=cache,
		key=relay_lock_key(),
		ttl_seconds=10,
		holder_id=holder_id,
	)


def test_lock_implements_the_contract(cache: MemoryCacheClient) -> None:
	assert isinstance(_lock(cache, 'h1'), BaseDistributedLock)


# --- mutual exclusion --------------------------------------------------------


@pytest.mark.asyncio
async def test_only_one_holder_wins(cache: MemoryCacheClient) -> None:
	first = _lock(cache, 'holder-1')
	second = _lock(cache, 'holder-2')

	assert await first.acquire() is True
	assert await second.acquire() is False
	assert first.is_held is True
	assert second.is_held is False


@pytest.mark.asyncio
async def test_a_holder_can_reacquire_after_releasing(cache: MemoryCacheClient) -> None:
	lock = _lock(cache, 'holder-1')

	assert await lock.acquire() is True
	await lock.release()

	assert lock.is_held is False
	assert await lock.acquire() is True


# --- expiry ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_expired_lease_is_reacquirable(cache: MemoryCacheClient, clock: FakeClock) -> None:
	first = _lock(cache, 'holder-1')
	second = _lock(cache, 'holder-2')
	await first.acquire()

	# holder-1 stalls past its TTL (long GC pause, blocked event loop).
	clock.advance(11)

	assert await second.acquire() is True
	# The old holder still *thinks* it holds the lease until it tries to renew.
	assert await first.renew() is False
	assert first.is_held is False


@pytest.mark.asyncio
async def test_renew_extends_the_lease(cache: MemoryCacheClient, clock: FakeClock) -> None:
	first = _lock(cache, 'holder-1')
	second = _lock(cache, 'holder-2')
	await first.acquire()

	clock.advance(8)
	assert await first.renew() is True
	clock.advance(8)

	# Without the renew the lease would have expired at t=10; the renew at t=8
	# pushed it to t=18, so the competitor is still locked out.
	assert await second.acquire() is False
	assert first.is_held is True


@pytest.mark.asyncio
async def test_renew_without_holding_is_refused(cache: MemoryCacheClient) -> None:
	lock = _lock(cache, 'holder-1')

	assert await lock.renew() is False
	assert lock.is_held is False


# --- owner-checked release ---------------------------------------------------


@pytest.mark.asyncio
async def test_release_only_drops_your_own_lease(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	first = _lock(cache, 'holder-1')
	second = _lock(cache, 'holder-2')
	await first.acquire()
	clock.advance(11)
	await second.acquire()

	# holder-1's release must NOT free the lease holder-2 now owns.
	await first.release()
	clock.advance(1)

	assert await _lock(cache, 'holder-3').acquire() is False
	assert second.is_held is True


@pytest.mark.asyncio
async def test_release_without_holding_is_safe(cache: MemoryCacheClient) -> None:
	lock = _lock(cache, 'holder-1')

	await lock.release()

	assert lock.is_held is False


# --- cadence -----------------------------------------------------------------


def test_renew_interval_is_a_third_of_the_ttl(cache: MemoryCacheClient) -> None:
	lock = ValkeyLeaseLock(cache=cache, key=relay_lock_key(), ttl_seconds=9)

	assert lock.renew_interval == 3


def test_renew_interval_has_a_floor(cache: MemoryCacheClient) -> None:
	lock = ValkeyLeaseLock(cache=cache, key=relay_lock_key(), ttl_seconds=1)

	assert lock.renew_interval == MIN_RENEW_INTERVAL


def test_holder_ids_are_unique_per_lock(cache: MemoryCacheClient) -> None:
	first = ValkeyLeaseLock(cache=cache, key=relay_lock_key(), ttl_seconds=10)
	second = ValkeyLeaseLock(cache=cache, key=relay_lock_key(), ttl_seconds=10)

	# The holder id is what makes the renew/release comparisons meaningful.
	assert first.holder_id != second.holder_id


def test_lock_key_layout_is_stable() -> None:
	# The key is the contract between the relay and whatever inspects Valkey.
	assert relay_lock_key() == 'lock:outbox-relay'

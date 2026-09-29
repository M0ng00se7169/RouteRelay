"""In-memory cache client contract tests (ADR-0008, Chunk 1).

These pin the semantics the Valkey client must match, because the whole test
suite (and the feature-flag-off production path) treats this class as a faithful
stand-in: TTL expiry, the permanent version counter, whole-key hash expiry and
the owner-checked lease primitives.
"""

import pytest

from infrastructure.cache.memory import MemoryCacheClient


class FakeClock:
	"""Manually advanced monotonic clock, so TTLs expire without sleeping."""

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


# --- strings -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_then_get_round_trips_bytes(cache: MemoryCacheClient) -> None:
	await cache.set('k', b'v')

	assert await cache.get('k') == b'v'


@pytest.mark.asyncio
async def test_get_missing_key_is_a_miss(cache: MemoryCacheClient) -> None:
	assert await cache.get('nope') is None
	assert await cache.exists('nope') is False


@pytest.mark.asyncio
async def test_ttl_expiry_hides_the_value(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	await cache.set('k', b'v', ttl_seconds=10)

	clock.advance(9)
	assert await cache.get('k') == b'v'

	clock.advance(2)
	assert await cache.get('k') is None
	assert await cache.exists('k') is False


@pytest.mark.asyncio
async def test_no_ttl_never_expires(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	await cache.set('k', b'v')

	clock.advance(10_000)

	assert await cache.get('k') == b'v'


@pytest.mark.asyncio
async def test_delete_removes_and_is_idempotent(cache: MemoryCacheClient) -> None:
	await cache.set('k', b'v')

	await cache.delete('k')
	assert await cache.get('k') is None

	# Deleting twice is a no-op, not an error.
	await cache.delete('k')


# --- counters ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_increment_starts_at_one_and_never_expires(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	# Generation 0 must not collide with "no generation yet" (ADR-0008 §2.3).
	assert await cache.increment('chat:ver:c1') == 1
	assert await cache.increment('chat:ver:c1') == 2

	# A TTL on the version key would silently reset the generation and make
	# stale cache pages current again, so counters are permanent.
	clock.advance(10_000)
	assert await cache.increment('chat:ver:c1') == 3


# --- hashes ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_counts_fields(cache: MemoryCacheClient) -> None:
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)
	await cache.hash_set('presence:c1', 'socket-2', '1', ttl_seconds=30)

	assert await cache.hash_count('presence:c1') == 2


@pytest.mark.asyncio
async def test_hash_set_is_idempotent_per_field(cache: MemoryCacheClient) -> None:
	# A heartbeat re-writes the same field; it must refresh, not duplicate.
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)

	assert await cache.hash_count('presence:c1') == 1


@pytest.mark.asyncio
async def test_hash_delete_removes_one_field(cache: MemoryCacheClient) -> None:
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)
	await cache.hash_set('presence:c1', 'socket-2', '1', ttl_seconds=30)

	await cache.hash_delete('presence:c1', 'socket-1')

	assert await cache.hash_count('presence:c1') == 1
	# Removing an unknown field is a no-op.
	await cache.hash_delete('presence:c1', 'ghost')


@pytest.mark.asyncio
async def test_heartbeat_rearms_the_whole_hash_ttl(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	# The self-healing property of presence: a socket that keeps beating keeps
	# the key alive; once it stops, the key (and its stale fields) expires.
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)

	clock.advance(20)
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)

	clock.advance(20)
	assert await cache.hash_count('presence:c1') == 1

	clock.advance(11)
	assert await cache.hash_count('presence:c1') == 0


# --- lease primitives --------------------------------------------------------


@pytest.mark.asyncio
async def test_set_if_absent_is_exclusive(cache: MemoryCacheClient) -> None:
	assert await cache.set_if_absent('lock', b'holder-1', ttl_milliseconds=10_000) is True
	assert await cache.set_if_absent('lock', b'holder-2', ttl_milliseconds=10_000) is False


@pytest.mark.asyncio
async def test_expired_lease_can_be_reacquired(
	cache: MemoryCacheClient,
	clock: FakeClock,
) -> None:
	await cache.set_if_absent('lock', b'holder-1', ttl_milliseconds=10_000)

	clock.advance(11)

	assert await cache.set_if_absent('lock', b'holder-2', ttl_milliseconds=10_000) is True


@pytest.mark.asyncio
async def test_compare_and_extend_only_for_the_owner(cache: MemoryCacheClient) -> None:
	await cache.set_if_absent('lock', b'holder-1', ttl_milliseconds=10_000)

	assert await cache.compare_and_extend('lock', b'holder-1', ttl_milliseconds=10_000) is True
	# A process that lost the lease must not resurrect it.
	assert await cache.compare_and_extend('lock', b'holder-2', ttl_milliseconds=10_000) is False


@pytest.mark.asyncio
async def test_compare_and_delete_only_for_the_owner(cache: MemoryCacheClient) -> None:
	await cache.set_if_absent('lock', b'holder-1', ttl_milliseconds=10_000)

	assert await cache.compare_and_delete('lock', b'holder-2') is False
	assert await cache.exists('lock') is True

	assert await cache.compare_and_delete('lock', b'holder-1') is True
	assert await cache.exists('lock') is False


@pytest.mark.asyncio
async def test_aclose_clears_everything_and_is_idempotent(cache: MemoryCacheClient) -> None:
	await cache.set('k', b'v')
	await cache.hash_set('presence:c1', 'socket-1', '1', ttl_seconds=30)

	await cache.aclose()
	await cache.aclose()

	assert await cache.get('k') is None
	assert await cache.hash_count('presence:c1') == 0

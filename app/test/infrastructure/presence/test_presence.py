"""Presence tracker tests (ADR-0008, Chunk 4).

The tracker is exercised against the in-memory store with a fake clock, so the
TTL semantics that make a crashed process self-healing are deterministic: a
socket that keeps beating stays counted, a socket that stops is collected by
the hash TTL, and a clean disconnect is removed immediately.
"""

import pytest

from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.presence.base import BasePresenceTracker
from infrastructure.presence.memory import MemoryPresenceTracker
from infrastructure.presence.valkey import (
	MIN_HEARTBEAT_INTERVAL,
	ValkeyPresenceTracker,
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
def tracker(clock: FakeClock) -> MemoryPresenceTracker:
	return MemoryPresenceTracker(cache=MemoryCacheClient(clock=clock), ttl_seconds=30)


def test_tracker_implements_the_contract(tracker: MemoryPresenceTracker) -> None:
	assert isinstance(tracker, BasePresenceTracker)


# --- counting ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_unknown_chat_has_no_presence(tracker: MemoryPresenceTracker) -> None:
	assert await tracker.count('c1') == 0


@pytest.mark.asyncio
async def test_registered_sockets_are_counted(tracker: MemoryPresenceTracker) -> None:
	await tracker.register('c1', 'socket-1')
	await tracker.register('c1', 'socket-2')

	assert await tracker.count('c1') == 2


@pytest.mark.asyncio
async def test_presence_is_scoped_per_chat(tracker: MemoryPresenceTracker) -> None:
	await tracker.register('c1', 'socket-1')
	await tracker.register('c2', 'socket-2')

	assert await tracker.count('c1') == 1
	assert await tracker.count('c2') == 1


@pytest.mark.asyncio
async def test_remove_takes_a_socket_off_presence(tracker: MemoryPresenceTracker) -> None:
	await tracker.register('c1', 'socket-1')
	await tracker.register('c1', 'socket-2')

	await tracker.remove('c1', 'socket-1')

	assert await tracker.count('c1') == 1


@pytest.mark.asyncio
async def test_removing_an_unknown_socket_is_a_noop(tracker: MemoryPresenceTracker) -> None:
	await tracker.register('c1', 'socket-1')

	await tracker.remove('c1', 'ghost')
	await tracker.remove('other-chat', 'socket-1')

	assert await tracker.count('c1') == 1


# --- TTL / heartbeat ---------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_keeps_a_socket_alive(
	tracker: MemoryPresenceTracker,
	clock: FakeClock,
) -> None:
	await tracker.register('c1', 'socket-1')

	# Three beats at ttl/3 is the whole TTL; one more and it would be gone.
	for _ in range(2):
		clock.advance(10)
		await tracker.refresh('c1', 'socket-1')

	assert await tracker.count('c1') == 1


@pytest.mark.asyncio
async def test_a_dead_socket_expires_with_the_ttl(
	tracker: MemoryPresenceTracker,
	clock: FakeClock,
) -> None:
	# The self-healing property: an abruptly killed process leaves a stale
	# field, and the hash TTL garbage-collects it. No cleanup job exists.
	await tracker.register('c1', 'socket-1')

	clock.advance(31)

	assert await tracker.count('c1') == 0


@pytest.mark.asyncio
async def test_one_stale_socket_does_not_evict_the_live_one(
	tracker: MemoryPresenceTracker,
	clock: FakeClock,
) -> None:
	await tracker.register('c1', 'dead-socket')
	await tracker.register('c1', 'live-socket')

	# The live socket keeps beating, so the key TTL is re-armed and the dead
	# field is only collected when the whole hash finally lapses. Documented
	# over-count window: bounded by the last heartbeat of the survivors.
	for _ in range(3):
		clock.advance(10)
		await tracker.refresh('c1', 'live-socket')

	assert await tracker.count('c1') == 2


# --- heartbeat cadence -------------------------------------------------------


def test_heartbeat_interval_is_a_third_of_the_ttl() -> None:
	tracker = ValkeyPresenceTracker(cache=MemoryCacheClient(), ttl_seconds=30)

	assert tracker.heartbeat_interval == 10


def test_heartbeat_interval_has_a_floor() -> None:
	# A 1s TTL must not become a 0.33s write loop against the cache.
	tracker = ValkeyPresenceTracker(cache=MemoryCacheClient(), ttl_seconds=1)

	assert tracker.heartbeat_interval == MIN_HEARTBEAT_INTERVAL

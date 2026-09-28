"""Valkey lease lock (ADR-0008, Chunk 5).

A lease built from three primitives of ``BaseCacheClient``, which map to
Valkey commands:

===========================  ==========================
acquire                      ``SET key <holder> NX PX <ttl>``
renew                        Lua compare-and-PEXPIRE
release                      Lua compare-and-DEL
===========================  ==========================

The holder id is a per-process uuid4: it makes the renew/release comparisons
meaningful ("is this still MY lease?") and it is deliberately NOT exported as a
metric label — holder ids are unbounded (D3) and knowing which replica leads is
not actionable for a demo stack.

The TTL is short (10s by default) and renewed every ``ttl / 3``. A lease must
expire quickly after a leader dies: until it does, the surviving replicas skip
their ticks and the outbox simply waits, which is at-least-once behaviour and
not a stall.
"""

from dataclasses import (
	dataclass,
	field,
)
from uuid import uuid4

from infrastructure.cache.base import BaseCacheClient
from infrastructure.locks.base import BaseDistributedLock

# Floor for the renew interval, so a tiny configured TTL cannot turn into a
# tight renew loop.
MIN_RENEW_INTERVAL = 0.5


@dataclass
class ValkeyLeaseLock(BaseDistributedLock):
	"""Single-holder lease with compare-and-extend renew."""

	cache: BaseCacheClient
	key: str
	ttl_seconds: int = 10
	# Per-process identity: it is what makes the renew/release comparisons
	# answer "is this still MY lease?", and it lets tests force a specific
	# holder id to simulate two competing processes.
	holder_id: str = field(default_factory=lambda: str(uuid4()))
	_is_held: bool = field(default=False, init=False, repr=False)

	@property
	def renew_interval(self) -> float:
		"""How often the holder should extend its lease."""
		return max(MIN_RENEW_INTERVAL, self.ttl_seconds / 3)

	@property
	def is_held(self) -> bool:
		return self._is_held

	@property
	def _value(self) -> bytes:
		return self.holder_id.encode()

	async def acquire(self) -> bool:
		self._is_held = await self.cache.set_if_absent(
			key=self.key,
			value=self._value,
			ttl_milliseconds=self.ttl_seconds * 1000,
		)
		return self._is_held

	async def renew(self) -> bool:
		if not self._is_held:
			return False
		self._is_held = await self.cache.compare_and_extend(
			key=self.key,
			value=self._value,
			ttl_milliseconds=self.ttl_seconds * 1000,
		)
		return self._is_held

	async def release(self) -> None:
		# compare_and_delete is a no-op (False) when the lease already expired
		# and was taken by someone else, which is exactly what we want.
		await self.cache.compare_and_delete(key=self.key, value=self._value)
		self._is_held = False

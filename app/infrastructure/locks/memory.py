"""In-memory lease lock: test double and flag-off fallback.

Wraps ``MemoryCacheClient``, whose ``set_if_absent`` / ``compare_and_extend`` /
``compare_and_delete`` reproduce the Valkey semantics the real lock relies on
(atomic create-if-absent, owner-checked renew, owner-checked release) — so the
lock's own logic, including its ``is_held`` bookkeeping, is exercised identically,
with an injectable clock standing in for lease expiry.

The single ``ValkeyLeaseLock`` instance is created once and reused: it owns the
held/not-held state, so a fresh instance per call would report ``is_held`` as
False forever.
"""

from dataclasses import (
	dataclass,
	field,
)
from uuid import uuid4

from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.locks.base import BaseDistributedLock
from infrastructure.locks.valkey import ValkeyLeaseLock


@dataclass
class MemoryLeaseLock(BaseDistributedLock):
	"""Lease over an in-process store with the same semantics."""

	cache: MemoryCacheClient
	key: str
	ttl_seconds: int = 10
	holder_id: str = field(default_factory=lambda: str(uuid4()))
	_inner: ValkeyLeaseLock = field(init=False, repr=False)

	def __post_init__(self) -> None:
		self._inner = ValkeyLeaseLock(
			cache=self.cache,
			key=self.key,
			ttl_seconds=self.ttl_seconds,
			holder_id=self.holder_id,
		)

	@property
	def is_held(self) -> bool:
		return self._inner.is_held

	@property
	def renew_interval(self) -> float:
		return self._inner.renew_interval

	async def acquire(self) -> bool:
		return await self._inner.acquire()

	async def renew(self) -> bool:
		return await self._inner.renew()

	async def release(self) -> None:
		await self._inner.release()

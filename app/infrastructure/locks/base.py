"""Distributed lock abstraction (ADR-0008, Chunk 5).

One implementation is needed today — the outbox relay's leader lock — so the
ABC exists to keep the relay testable and to make the contract explicit rather
than to enable polymorphism.

**The contract.** A lock is a lease with a TTL, not a mutex:

- ``acquire`` succeeds only if nobody holds it, atomically.
- ``renew`` extends the lease ONLY while this holder still owns it. A process
  that stalled past its own TTL must discover it lost the lock instead of
  silently reclaiming it from whoever took over.
- ``release`` drops it only if still owned, for the same reason.

The "only if owned" clause on renew and release is the whole point: a relay that
paused long enough to lose its lease can come back and must not extend or delete
a lock that now belongs to another process.

Losing a lock is always safe for the outbox — it is at-least-once by contract
(``OutboxRelay.run``'s docstring), so a second publisher duplicates rows, and
duplicates are an expected replay case.
"""

from abc import (
	ABC,
	abstractmethod,
)


class BaseDistributedLock(ABC):
	"""TTL lease held by a single holder id."""

	@property
	@abstractmethod
	def is_held(self) -> bool:
		"""Whether THIS holder currently believes it owns the lease."""

	@abstractmethod
	async def acquire(self) -> bool:
		"""Try to take the lease. False means somebody else has it."""

	@abstractmethod
	async def renew(self) -> bool:
		"""Extend the lease. False means it was lost and must be re-acquired."""

	@abstractmethod
	async def release(self) -> None:
		"""Give the lease up. Safe to call when not held, and on shutdown."""

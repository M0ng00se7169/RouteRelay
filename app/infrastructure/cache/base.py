"""Cache client abstraction (ADR-0008).

Valkey backs three features — cache-aside for hot reads, chat presence, and the
outbox relay leader lock — but the application layer only ever sees this ABC.
The concrete Valkey client (infrastructure/cache/valkey.py) is the ONLY module
that imports ``valkey``; everything downstream is written against the methods
below, which is what makes the in-memory double (memory.py) a faithful test
substitute rather than a mock with different semantics.

Contract every implementation must honour:

- **Best effort.** A cache call never fails the caller. The Valkey client wraps
  every operation in its circuit breaker, counts failures on
  ``cache_errors_total``, logs a warning and returns the *same value the caller
  would see on a cold cache* (None / False / 0 / no-op). The proxies in
  cached.py therefore never need try/except, and a Valkey outage degrades read
  latency rather than availability.
- **Bytes in, bytes out.** Values are opaque ``bytes``; serialization lives in
  the calling proxy so this layer stays a plain key-value store.
- **Explicit teardown.** ``aclose()`` releases the connection pool; asyncio
  clients are not disconnected by the garbage collector. The lifespan calls it
  on shutdown.

The method set deliberately covers the three key shapes the ADR defines
(§2.3): plain strings (cached reads), a counter (the versioned-invalidation
generation) and a hash (presence). The two lock primitives at the bottom are
``SET NX PX`` and its compare-and-extend / compare-and-delete counterparts —
they live here so the lease implementation (infrastructure/locks) can stay free
of any Valkey import too.
"""

from abc import (
	ABC,
	abstractmethod,
)


class BaseCacheClient(ABC):
	"""Async key-value client with TTLs, counters, hashes and a lease primitive."""

	# --- strings (cache-aside reads) ---------------------------------------

	@abstractmethod
	async def get(self, key: str) -> bytes | None:
		"""Return the raw value for ``key``, or None on a miss.

		A cache *error* also returns None (see the module docstring), which is
		indistinguishable from a miss by design — the caller then reads Mongo.
		"""

	@abstractmethod
	async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
		"""Store ``value`` under ``key``, optionally with a TTL in seconds.

		``ttl_seconds=None`` means "no expiry".
		"""

	@abstractmethod
	async def delete(self, key: str) -> None:
		"""Delete ``key``. Deleting a missing key is a no-op, not an error."""

	@abstractmethod
	async def exists(self, key: str) -> bool:
		"""Whether ``key`` is present. Errors return False (cache-aside)."""

	# --- counters (versioned invalidation) ----------------------------------

	@abstractmethod
	async def increment(self, key: str) -> int:
		"""Increment the counter at ``key`` and return its new value.

		Never creates an expiring entry: the version key is permanent and
		``DeleteChatCommandHandler`` removes it with the chat.
		"""

	# --- hashes (chat presence) ---------------------------------------------

	@abstractmethod
	async def hash_set(
		self,
		key: str,
		field: str,
		value: str,
		ttl_seconds: int | None = None,
	) -> None:
		"""Set one hash field and (re)arm the key's TTL.

		Presence stores one field per live socket and refreshes the whole key's
		TTL on every heartbeat, so an abruptly dead process leaves a stale field
		that expires with the key — no cleanup job needed. Field values are text
		(presence metadata), unlike the binary payloads of the string keys above.
		"""

	@abstractmethod
	async def hash_count(self, key: str) -> int:
		"""Number of fields in the hash at ``key`` (0 when absent/errored)."""

	@abstractmethod
	async def hash_delete(self, key: str, field: str) -> None:
		"""Remove one field. A missing field or key is a no-op."""

	# --- lease primitives (outbox relay leader lock) -------------------------

	@abstractmethod
	async def set_if_absent(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		"""``SET key value NX PX ttl``: True only if the key was created here.

		The atomicity of this single command is what makes the relay lock safe
		against two processes racing at the same instant.
		"""

	@abstractmethod
	async def compare_and_extend(
		self,
		key: str,
		value: bytes,
		ttl_milliseconds: int,
	) -> bool:
		"""Re-arm the TTL only if ``key`` still holds ``value``; else False.

		The renew half of the lease: a process that lost the lock (expired and
		taken over by someone else) must not resurrect its own lease.
		"""

	@abstractmethod
	async def compare_and_delete(self, key: str, value: bytes) -> bool:
		"""Delete ``key`` only if it still holds ``value``; else False.

		The release half: a lock that expired and was re-acquired elsewhere must
		not be deleted by its previous holder.
		"""

	# --- lifecycle ------------------------------------------------------------

	@abstractmethod
	async def aclose(self) -> None:
		"""Release connections. Must be idempotent and never raise."""

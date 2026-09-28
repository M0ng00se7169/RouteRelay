"""In-memory cache client: the test double AND the production fallback.

Two jobs, per ADR-0008 §2.2:

1. Every test uses this instead of Valkey, so the test suite stays
   service-free (the same reason the repositories have ``Memory*``
   implementations).
2. With ``CACHE_ENABLED=false`` the container registers this client, so
   handlers and proxies never branch on ``None`` — the cached path stays
   wired and exercised, just against a dictionary.

Expiry uses ``time.monotonic()``: wall-clock jumps (NTP steps, suspend/resume)
must not resurrect or prematurely expire cache entries, and the tests inject a
fake clock to advance TTLs deterministically.
"""

from collections.abc import Callable
from dataclasses import (
	dataclass,
	field,
)
from time import monotonic

from infrastructure.cache.base import BaseCacheClient


@dataclass
class _Entry:
	value: bytes
	expires_at: float | None = None


@dataclass
class MemoryCacheClient(BaseCacheClient):
	"""dict-backed client honouring the BaseCacheClient contract."""

	# Injectable clock so tests can expire entries without sleeping.
	clock: Callable[[], float] = monotonic
	_strings: dict[str, _Entry] = field(default_factory=dict, repr=False)
	_hashes: dict[str, dict[str, _Entry]] = field(default_factory=dict, repr=False)
	# Per-hash expiry, refreshed on every hash_set (Valkey arms a TTL on the
	# whole key, so a crashed socket's field dies with the key).
	_hash_expiry: dict[str, float | None] = field(default_factory=dict, repr=False)

	# --- expiry -------------------------------------------------------------

	def _alive(self, entry: _Entry) -> bool:
		return entry.expires_at is None or entry.expires_at > self.clock()

	def _live_string(self, key: str) -> _Entry | None:
		"""Return the entry for ``key``, dropping it if its TTL elapsed."""
		entry = self._strings.get(key)
		if entry is None:
			return None
		if not self._alive(entry):
			self._strings.pop(key, None)
			return None
		return entry

	def _live_hash(self, key: str) -> dict[str, _Entry]:
		"""Return the live fields of ``key``, dropping the hash if it expired."""
		if key in self._hashes:
			expires_at = self._hash_expiry.get(key)
			if expires_at is not None and expires_at <= self.clock():
				self._hashes.pop(key, None)
				self._hash_expiry.pop(key, None)
		return self._hashes.setdefault(key, {})

	# --- strings (cache-aside reads) ---------------------------------------

	async def get(self, key: str) -> bytes | None:
		entry = self._live_string(key)
		return None if entry is None else entry.value

	async def set(self, key: str, value: bytes, ttl_seconds: int | None = None) -> None:
		expires_at = None if ttl_seconds is None else self.clock() + ttl_seconds
		self._strings[key] = _Entry(value=value, expires_at=expires_at)

	async def delete(self, key: str) -> None:
		self._strings.pop(key, None)

	async def exists(self, key: str) -> bool:
		return self._live_string(key) is not None

	# --- counters (versioned invalidation) ---------------------------------

	async def increment(self, key: str) -> int:
		# Counters are permanent: a TTL here would silently reset the generation
		# and let stale cache page keys become current again.
		raw = await self.get(key)
		new_value = (0 if raw is None else int(raw)) + 1
		self._strings[key] = _Entry(value=str(new_value).encode())
		return new_value

	# --- hashes (chat presence) --------------------------------------------

	async def hash_set(
		self,
		key: str,
		field: str,
		value: str,
		ttl_seconds: int | None = None,
	) -> None:
		now = self.clock()
		self._live_hash(key)[field] = _Entry(value=value.encode())
		self._hash_expiry[key] = None if ttl_seconds is None else now + ttl_seconds

	async def hash_count(self, key: str) -> int:
		return len(self._live_hash(key))

	async def hash_delete(self, key: str, field: str) -> None:
		fields = self._live_hash(key)
		fields.pop(field, None)
		if not fields:
			self._hash_expiry.pop(key, None)

	# --- lease primitives ---------------------------------------------------

	async def set_if_absent(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		if self._live_string(key) is not None:
			return False
		# Milliseconds, exactly like the Valkey SET ... PX the client sends.
		self._strings[key] = _Entry(
			value=value,
			expires_at=self.clock() + ttl_milliseconds / 1000,
		)
		return True

	async def compare_and_extend(self, key: str, value: bytes, ttl_milliseconds: int) -> bool:
		entry = self._live_string(key)
		if entry is None or entry.value != value:
			return False
		entry.expires_at = self.clock() + ttl_milliseconds / 1000
		return True

	async def compare_and_delete(self, key: str, value: bytes) -> bool:
		entry = self._live_string(key)
		if entry is None or entry.value != value:
			return False
		self._strings.pop(key, None)
		return True

	# --- lifecycle ----------------------------------------------------------

	async def aclose(self) -> None:
		self._strings.clear()
		self._hashes.clear()
		self._hash_expiry.clear()

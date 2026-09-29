"""In-memory presence tracker: test double and flag-off fallback.

Same role as ``MemoryCacheClient`` — the tests get presence coverage without a
cache server, and with ``PRESENCE_ENABLED=false`` the container still resolves
a tracker so the query handler never branches on ``None`` (it simply always
reports 0).

Built on ``MemoryCacheClient``'s hash primitives so TTL semantics (including
the "whole key expires" behaviour that makes a crashed process self-heal) are
identical to the Valkey tracker's — the tests then exercise the real code path.
"""

from dataclasses import dataclass

from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.presence.base import BasePresenceTracker
from infrastructure.presence.valkey import ValkeyPresenceTracker


@dataclass
class MemoryPresenceTracker(BasePresenceTracker):
	"""Presence over an in-process store with the same TTL semantics."""

	cache: MemoryCacheClient
	ttl_seconds: int = 30

	async def register(self, chat_oid: str, socket_id: str) -> None:
		await self._valkey().register(chat_oid, socket_id)

	async def refresh(self, chat_oid: str, socket_id: str) -> None:
		await self._valkey().refresh(chat_oid, socket_id)

	async def remove(self, chat_oid: str, socket_id: str) -> None:
		await self._valkey().remove(chat_oid, socket_id)

	async def count(self, chat_oid: str) -> int:
		return await self._valkey().count(chat_oid)

	def _valkey(self) -> ValkeyPresenceTracker:
		# The tracker logic (keys, TTL re-arm policy) is shared with the real
		# implementation; only the store differs, so the double cannot drift.
		return ValkeyPresenceTracker(cache=self.cache, ttl_seconds=self.ttl_seconds)

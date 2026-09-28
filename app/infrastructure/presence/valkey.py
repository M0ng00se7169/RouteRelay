"""Valkey-backed presence tracker (ADR-0008 §2.3, Chunk 4).

One hash per chat (``presence:{chat_oid}``) with a field per socket id. The
TTL is armed on the whole key and refreshed by every heartbeat, which is what
makes abrupt process death self-healing: a socket that stops beating stops
extending the key, and the key — with its stale fields — expires. No sweeper,
no cleanup task, nothing to reconcile after a crash.

The heartbeat interval is ``ttl / 3``, so two consecutive missed beats still
leave a margin before the entry lapses while a busy chat is never written more
than three times per TTL.
"""

from dataclasses import dataclass
from time import time

from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.keys import presence_cache_key
from infrastructure.presence.base import BasePresenceTracker

# Minimum beat interval, so a misconfigured TTL of 1s cannot turn into a
# 0.33s write loop against the cache.
MIN_HEARTBEAT_INTERVAL = 1.0


@dataclass
class ValkeyPresenceTracker(BasePresenceTracker):
	"""Presence as a TTL'd hash, one field per live socket."""

	cache: BaseCacheClient
	ttl_seconds: int = 30

	@property
	def heartbeat_interval(self) -> float:
		"""How often a socket should refresh its presence entry."""
		return max(MIN_HEARTBEAT_INTERVAL, self.ttl_seconds / 3)

	async def register(self, chat_oid: str, socket_id: str) -> None:
		await self.cache.hash_set(
			key=presence_cache_key(chat_oid),
			field=socket_id,
			# The value carries the last-seen timestamp. Nothing reads it today,
			# but it makes the entry inspectable (`valkey-cli hgetall`) and gives a
			# future "who is online" endpoint a sort order for free.
			value=str(time()),
			ttl_seconds=self.ttl_seconds,
		)

	async def refresh(self, chat_oid: str, socket_id: str) -> None:
		# Same write as register(): HSET is idempotent per field and the point
		# of the call is the TTL re-arm.
		await self.register(chat_oid, socket_id)

	async def remove(self, chat_oid: str, socket_id: str) -> None:
		await self.cache.hash_delete(key=presence_cache_key(chat_oid), field=socket_id)

	async def count(self, chat_oid: str) -> int:
		return await self.cache.hash_count(key=presence_cache_key(chat_oid))

"""Presence tracker abstraction (ADR-0008, Chunk 4).

Chat presence answers "how many sockets are currently attached to this chat".
Before ADR-0008 the only connection state was ``ConnectionManager``'s
in-process ``dict[str, list[WebSocket]]`` — invisible to the API layer and
meaningless across processes.

The ABC is intentionally tiny: register, refresh, remove, count. Every call is
best-effort (a presence miss must never fail a request), and the implementation
is TTL-driven so no cleanup job is needed.
"""

from abc import (
	ABC,
	abstractmethod,
)


class BasePresenceTracker(ABC):
	"""Per-chat set of live socket ids, garbage-collected by TTL."""

	@abstractmethod
	async def register(self, chat_oid: str, socket_id: str) -> None:
		"""Record ``socket_id`` as attached to the chat and arm the TTL."""

	@abstractmethod
	async def refresh(self, chat_oid: str, socket_id: str) -> None:
		"""Re-arm the TTL for an already registered socket (the heartbeat)."""

	@abstractmethod
	async def remove(self, chat_oid: str, socket_id: str) -> None:
		"""Forget a socket. Removing an unknown socket is a no-op."""

	@abstractmethod
	async def count(self, chat_oid: str) -> int:
		"""Number of sockets currently considered live for the chat.

		A cache error returns 0: "we cannot tell" degrades to "nobody is here",
		which is the non-alarming direction for a presence indicator.
		"""

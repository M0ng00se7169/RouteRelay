from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from datetime import datetime

from domain.events.base import BaseEvent


@dataclass
class OutboxRow:
	_id: str
	event_id: str
	topic: str
	key: bytes
	payload: bytes
	occurred_at: datetime
	sent: bool = False
	sent_at: datetime | None = None


@dataclass
class BaseOutboxRepository(ABC):
	@abstractmethod
	async def save_events(self, events: list[BaseEvent], session=None) -> None:
		...

	@abstractmethod
	async def get_unsent(self, limit: int) -> list[OutboxRow]:
		...

	@abstractmethod
	async def count_unsent(self) -> int:
		"""Total number of unsent rows in the outbox (no limit).

		Used by the relay to report the true backlog via the ``outbox_pending``
		gauge — ``get_unsent(limit)`` is capped at the relay batch size and
		under-reports once the backlog exceeds it (ADR-0006, G10).
		"""
		...

	@abstractmethod
	async def mark_as_sent(self, ids: list[str]) -> None:
		...

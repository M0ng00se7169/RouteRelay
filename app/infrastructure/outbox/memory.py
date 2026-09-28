from collections.abc import Callable
from dataclasses import (
	dataclass,
	field,
)
from datetime import (
	UTC,
	datetime,
)
from typing import Any
from uuid import uuid4

from domain.events.base import BaseEvent
from infrastructure.message_brokers.converters import convert_event_to_broker_message
from infrastructure.outbox.base import (
	BaseOutboxRepository,
	OutboxRow,
)


@dataclass
class MemoryOutboxRepository(BaseOutboxRepository):
	_outbox: list[OutboxRow] = field(default_factory=list)
	_topic_resolver: Callable[[BaseEvent], str] = lambda event: 'default-topic'

	async def save_events(self, events: list[BaseEvent], session: Any | None = None) -> None:
		for event in events:
			self._outbox.append(
				OutboxRow(
					_id=str(uuid4()),
					event_id=str(event.event_id),
					topic=self._topic_resolver(event),
					key=str(event.event_id).encode(),
					payload=convert_event_to_broker_message(event),
					occurred_at=event.occurred_at,
				),
			)

	async def get_unsent(self, limit: int) -> list[OutboxRow]:
		return [row for row in self._outbox if not row.sent][:limit]

	async def count_unsent(self) -> int:
		return sum(1 for row in self._outbox if not row.sent)

	async def mark_as_sent(self, ids: list[str]) -> None:
		sent_at = datetime.now(UTC)
		for row in self._outbox:
			if row._id in ids:
				row.sent = True
				row.sent_at = sent_at

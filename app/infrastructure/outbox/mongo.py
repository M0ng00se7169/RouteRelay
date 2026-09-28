from collections.abc import Callable
from dataclasses import dataclass
from datetime import (
	UTC,
	datetime,
)
from typing import Any
from uuid import uuid4

from motor.core import AgnosticCollection

from domain.events.base import BaseEvent
from infrastructure.message_brokers.converters import convert_event_to_broker_message
from infrastructure.outbox.base import (
	BaseOutboxRepository,
	OutboxRow,
)


@dataclass
class MongoOutboxRepository(BaseOutboxRepository):
	collection: AgnosticCollection
	_topic_resolver: Callable[[BaseEvent], str]

	async def save_events(self, events: list[BaseEvent], session=None) -> None:
		if not events:
			return
		docs = [
			{
				'_id': str(uuid4()),
				'event_id': str(event.event_id),
				'topic': self._topic_resolver(event),
				'key': str(event.event_id).encode(),
				'payload': convert_event_to_broker_message(event),
				'occurred_at': event.occurred_at,
				'sent': False,
				'sent_at': None,
			}
			for event in events
		]
		await self.collection.insert_many(docs, session=session)

	async def get_unsent(self, limit: int) -> list[OutboxRow]:
		cursor = self.collection.find({'sent': False}).limit(limit)
		return [self._to_row(doc) async for doc in cursor]

	async def count_unsent(self) -> int:
		# Uses the `sent` field — back it with a partial index on {'sent': False}
		# once the outbox grows large (ADR-0006, Section 7).
		return await self.collection.count_documents({'sent': False})

	async def mark_as_sent(self, ids: list[str]) -> None:
		await self.collection.update_many(
			{'_id': {'$in': ids}},
			{'$set': {'sent': True, 'sent_at': datetime.now(UTC)}},
		)

	@staticmethod
	def _to_row(doc: dict[str, Any]) -> OutboxRow:
		return OutboxRow(
			_id=str(doc['_id']),
			event_id=str(doc['event_id']),
			topic=doc['topic'],
			key=doc['key'],
			payload=doc['payload'],
			occurred_at=doc['occurred_at'],
			sent=doc['sent'],
			sent_at=doc.get('sent_at'),
		)

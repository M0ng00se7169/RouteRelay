import asyncio
import logging
from dataclasses import dataclass

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.metrics import (
    kafka_messages_sent_total,
    outbox_pending,
    outbox_publish_errors_total,
    outbox_published_total,
)
from infrastructure.outbox.base import BaseOutboxRepository

from settings.config import Config


logger = logging.getLogger(__name__)


@dataclass
class OutboxRelay:
	# The relay is the SOLE writer to Kafka. It polls the outbox collection for
	# unsent rows (written atomically with business data by the command handlers)
	# and publishes each row's payload to its topic. Delivery is at-least-once:
	# if the process crashes between send and mark_as_sent, a row may be re-sent
	# once on the next pass. Downstream consumers can dedupe on the row key
	# (str(event_id)), which is stable across replays.
	outbox_repository: BaseOutboxRepository
	message_broker: BaseMessageBroker
	poll_interval: float = 1.0
	batch_size: int = 100

	async def _tick(self) -> None:
		rows = await self.outbox_repository.get_unsent(self.batch_size)
		outbox_pending.set(len(rows))
		if not rows:
			return

		sent_ids: list[str] = []
		for row in rows:
			try:
				# KafkaMessageBroker.send_message(self, key, topic, value)
				await self.message_broker.send_message(
					key=row.key,
					topic=row.topic,
					value=row.payload,
				)
				kafka_messages_sent_total.inc()
				sent_ids.append(row._id)
			except Exception:
				logger.exception('Failed to publish outbox row %s', row._id)
				outbox_publish_errors_total.inc()
				# Stop the batch on first failure; remaining rows stay unsent
				# and will be retried on the next tick.
				break

		if sent_ids:
			await self.outbox_repository.mark_as_sent(sent_ids)
			outbox_published_total.inc(len(sent_ids))

		# Recompute the pending gauge from the repo so it reflects the rows that
		# are still unsent after this tick (not the pre-send snapshot).
		outbox_pending.set(len(await self.outbox_repository.get_unsent(self.batch_size)))

	async def run(self) -> None:
		while True:
			await self._tick()
			await asyncio.sleep(self.poll_interval)


def build_relay(
	outbox_repository: BaseOutboxRepository,
	message_broker: BaseMessageBroker,
	config: Config,
) -> OutboxRelay:
	return OutboxRelay(
		outbox_repository=outbox_repository,
		message_broker=message_broker,
		poll_interval=config.outbox_relay_poll_interval,
	)

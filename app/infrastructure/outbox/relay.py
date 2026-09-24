import asyncio
import logging
from dataclasses import dataclass
from time import perf_counter

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.metrics import (
    kafka_messages_sent_total,
    outbox_pending,
    outbox_publish_duration_seconds,
    outbox_publish_errors_total,
    outbox_published_total,
    safe_inc,
    safe_observe,
    safe_set,
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
		# True backlog, not capped at batch_size (ADR-0006, G10/Chunk 2.1).
		safe_set(outbox_pending, await self.outbox_repository.count_unsent())
		rows = await self.outbox_repository.get_unsent(self.batch_size)
		if not rows:
			return

		sent_ids: list[str] = []
		for row in rows:
			# KafkaMessageBroker.send_message(self, key, topic, value)
			started_at = perf_counter()
			try:
				await self.message_broker.send_message(
					key=row.key,
					topic=row.topic,
					value=row.payload,
				)
				safe_inc(kafka_messages_sent_total, topic=row.topic)
				sent_ids.append(row._id)
			except Exception:
				logger.exception('Failed to publish outbox row %s', row._id)
				safe_inc(outbox_publish_errors_total)
				# Stop the batch on first failure; remaining rows stay unsent
				# and will be retried on the next tick.
				break
			finally:
				# Observe every attempt — success or failure — so the histogram
				# also reflects the latency of erroring/timed-out sends.
				safe_observe(
					outbox_publish_duration_seconds.labels(topic=row.topic).observe,
					perf_counter() - started_at,
				)

		if sent_ids:
			await self.outbox_repository.mark_as_sent(sent_ids)
			safe_inc(outbox_published_total, amount=len(sent_ids))

		# Recompute the pending gauge from the repo so it reflects the rows that
		# are still unsent after this tick (not the pre-send snapshot).
		safe_set(outbox_pending, await self.outbox_repository.count_unsent())

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

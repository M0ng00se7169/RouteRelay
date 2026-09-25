import asyncio
import logging
from dataclasses import dataclass
from time import perf_counter

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.metrics import (
    circuit_breaker_rejected_total,
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
from infrastructure.resilience import (
    CircuitBreaker,
    CircuitOpenError,
)

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
	# O-2: optional 'kafka' circuit breaker (built in logic/init.py's
	# create_outbox_relay). While it is open, the batch is skipped up front —
	# one warning per tick instead of a doomed producer timeout on row 1.
	# None disables the breaker (tests, dummy container).
	circuit_breaker: CircuitBreaker | None = None

	async def _tick(self) -> None:
		# True backlog, not capped at batch_size (ADR-0006, G10/Chunk 2.1).
		safe_set(outbox_pending, await self.outbox_repository.count_unsent())
		rows = await self.outbox_repository.get_unsent(self.batch_size)
		if not rows:
			return

		sent_ids: list[str] = []
		for row in rows:
			# Fail fast while the breaker is open (O-2): one rejection per tick
			# instead of a doomed send plus producer timeout. Rows stay unsent
			# and are retried on a later tick; the breaker's half-open probe
			# re-tests Kafka after the recovery window.
			if self.circuit_breaker is not None and self.circuit_breaker.state == 'open':
				logger.warning(
					'Kafka circuit open; skipping outbox batch, %s rows stay unsent',
					len(rows) - len(sent_ids),
				)
				safe_inc(circuit_breaker_rejected_total, name=self.circuit_breaker.name)
				break
			# KafkaMessageBroker.send_message(self, topic, key, value)
			started_at = perf_counter()
			rejected = False
			try:
				if self.circuit_breaker is not None:
					await self.circuit_breaker.call(
						lambda: self.message_broker.send_message(
							key=row.key,
							topic=row.topic,
							value=row.payload,
						),
					)
				else:
					await self.message_broker.send_message(
						key=row.key,
						topic=row.topic,
						value=row.payload,
					)
			except CircuitOpenError:
				# Lost the race: the breaker tripped between the pre-check and
				# the call. Nothing was attempted, so no histogram sample (a
				# ~0ms sample would pollute the timeout-latency distribution);
				# the breaker's own rejection counter was already incremented
				# inside call(). Stop the batch.
				rejected = True
				logger.warning('Kafka circuit tripped mid-batch; remaining rows stay unsent')
				break
			except Exception:
				logger.exception('Failed to publish outbox row %s', row._id)
				safe_inc(outbox_publish_errors_total)
				# Stop the batch on first failure; remaining rows stay unsent
				# and will be retried on the next tick.
				break
			finally:
				# Observe every real attempt — success or failure — so the
				# histogram also reflects the latency of erroring/timed-out
				# sends. Rejections are not attempts: no sample.
				if not rejected:
					safe_observe(
						outbox_publish_duration_seconds.labels(topic=row.topic).observe,
						perf_counter() - started_at,
					)
			safe_inc(kafka_messages_sent_total, topic=row.topic)
			sent_ids.append(row._id)

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
	circuit_breaker: CircuitBreaker | None = None,
) -> OutboxRelay:
	return OutboxRelay(
		outbox_repository=outbox_repository,
		message_broker=message_broker,
		poll_interval=config.outbox_relay_poll_interval,
		circuit_breaker=circuit_breaker,
	)

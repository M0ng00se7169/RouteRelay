from datetime import (
	UTC,
	datetime,
)
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.outbox.base import OutboxRow
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.relay import OutboxRelay
from infrastructure.resilience import CircuitBreaker

METRIC_NAMES = (
	'outbox_published_total',
	'outbox_publish_errors_total',
	'outbox_pending',
)

# kafka_messages_sent_total is topic-labelled (ADR-0006 Chunk 2.2): the
# label-less get_sample_value(name) returns None, so baselines/assertions are
# taken per topic. Topics used by the rows these tests create.
KAFKA_SENT_TOPICS = (
	'chat-events',
	'new-messages',
)


def _value(name: str) -> float:
	value = REGISTRY.get_sample_value(name)
	return value if value is not None else 0.0


def _hist_value(name: str, labels: dict, *, sample: str = '_count') -> float:
	# sample includes its own leading underscore (_count/_sum), e.g.
	# get_sample_value('outbox_publish_duration_seconds_count', {'topic': ...}).
	value = REGISTRY.get_sample_value(name + sample, labels)
	return value if value is not None else 0.0


def _kafka_sent_value(topic: str) -> float:
	value = REGISTRY.get_sample_value('kafka_messages_sent_total', {'topic': topic})
	return value if value is not None else 0.0


def _make_row(topic: str = 'chat-events', sent: bool = False) -> OutboxRow:
	return OutboxRow(
		_id=str(uuid4()),
		event_id=str(uuid4()),
		topic=topic,
		key=str(uuid4()).encode(),
		payload=b'{"event": "x"}',
		occurred_at=datetime.now(UTC),
		sent=sent,
	)


class FakeBroker(BaseMessageBroker):
	def __init__(self, fail: bool = False) -> None:
		self.fail = fail
		self.sent: list[tuple] = []

	async def send_message(self, key: bytes, topic: str, value: bytes) -> None:
		if self.fail:
			raise RuntimeError('kafka down')
		self.sent.append((topic, key, value))

	async def start(self) -> None:
		...

	async def close(self) -> None:
		...

	async def start_consuming(self, topic: str):
		...

	async def stop_consuming(self) -> None:
		...


def _build_relay(
	repo: MemoryOutboxRepository,
	broker: FakeBroker,
	circuit_breaker: CircuitBreaker | None = None,
) -> OutboxRelay:
	return OutboxRelay(
		outbox_repository=repo,
		message_broker=broker,
		poll_interval=0.0,
		batch_size=10,
		circuit_breaker=circuit_breaker,
	)


@pytest.fixture
def metric_baselines():
	baselines = {name: _value(name) for name in METRIC_NAMES}
	baselines['kafka_messages_sent_total'] = {
		topic: _kafka_sent_value(topic) for topic in KAFKA_SENT_TOPICS
	}
	return baselines


@pytest.mark.asyncio
async def test_relay_publishes_unsent_rows_and_marks_sent(metric_baselines):
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(), _make_row()])
	broker = FakeBroker()
	relay = _build_relay(repo, broker)

	await relay._tick()

	assert len(broker.sent) == 2
	remaining = await repo.get_unsent(10)
	assert remaining == []
	assert _value('outbox_published_total') - metric_baselines['outbox_published_total'] == 2
	assert (
		_kafka_sent_value('chat-events')
		- metric_baselines['kafka_messages_sent_total']['chat-events']
	) == 2
	assert _value('outbox_pending') == 0


@pytest.mark.asyncio
async def test_relay_counts_sends_per_topic(metric_baselines):
	# G9 (ADR-0006 Chunk 2.2): sends must be attributable per topic.
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(topic='chat-events'), _make_row(topic='new-messages')])
	broker = FakeBroker()
	relay = _build_relay(repo, broker)

	await relay._tick()

	assert (
		_kafka_sent_value('chat-events')
		- metric_baselines['kafka_messages_sent_total']['chat-events']
	) == 1
	assert (
		_kafka_sent_value('new-messages')
		- metric_baselines['kafka_messages_sent_total']['new-messages']
	) == 1


@pytest.mark.asyncio
async def test_relay_observes_publish_duration_per_topic():
	# Chunk 2.3 (ADR-0006): each successful send adds one observation per topic.
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(topic='chat-events'), _make_row(topic='new-messages')])
	broker = FakeBroker()
	relay = _build_relay(repo, broker)

	count_before = sum(
		_hist_value('outbox_publish_duration_seconds', {'topic': t})
		for t in ('chat-events', 'new-messages')
	)

	await relay._tick()

	count_after = sum(
		_hist_value('outbox_publish_duration_seconds', {'topic': t})
		for t in ('chat-events', 'new-messages')
	)
	assert count_after - count_before == 2
	for topic in ('chat-events', 'new-messages'):
		assert (
			_hist_value('outbox_publish_duration_seconds', {'topic': topic}, sample='_sum')
			> 0.0
		)


@pytest.mark.asyncio
async def test_relay_observes_duration_on_failed_send_too():
	# The histogram must capture failed attempts as well (timeout latency is
	# exactly what you need to see during a Kafka outage).
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(topic='chat-events')])
	broker = FakeBroker(fail=True)
	relay = _build_relay(repo, broker)

	count_before = _hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'})

	await relay._tick()

	assert (
		_hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'})
		- count_before
	) == 1
	assert (
		_hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'}, sample='_sum')
		> 0.0
	)


@pytest.mark.asyncio
async def test_relay_ignores_already_sent_rows(metric_baselines):
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(sent=True)])
	broker = FakeBroker()
	relay = _build_relay(repo, broker)

	await relay._tick()

	assert broker.sent == []
	assert _value('outbox_published_total') - metric_baselines['outbox_published_total'] == 0
	assert _value('outbox_pending') == 0


@pytest.mark.asyncio
async def test_relay_keeps_rows_unsent_on_broker_failure(metric_baselines):
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(), _make_row()])
	broker = FakeBroker(fail=True)
	relay = _build_relay(repo, broker)

	await relay._tick()

	assert broker.sent == []
	remaining = await repo.get_unsent(10)
	assert len(remaining) == 2
	assert _value('outbox_publish_errors_total') - metric_baselines['outbox_publish_errors_total'] == 1
	assert _value('outbox_published_total') - metric_baselines['outbox_published_total'] == 0
	assert (
		_kafka_sent_value('chat-events')
		- metric_baselines['kafka_messages_sent_total']['chat-events']
	) == 0


# --- kafka circuit breaker (O-2) ---------------------------------------------


def _rejected(name: str) -> float:
	value = REGISTRY.get_sample_value('circuit_breaker_rejected_total', {'name': name})
	return value if value is not None else 0.0


def _state_gauge(name: str) -> float:
	value = REGISTRY.get_sample_value('circuit_breaker_state', {'name': name})
	return value if value is not None else 0.0


@pytest.mark.asyncio
async def test_relay_trips_breaker_after_consecutive_failures():
	# The batch aborts on the FIRST failure, so each tick contributes exactly
	# one failure to the breaker (the remaining rows wait for the next tick).
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row() for _ in range(4)])
	broker = FakeBroker(fail=True)
	relay = _build_relay(
     repo, broker, circuit_breaker=CircuitBreaker(
      name='relay-test-trip', failure_threshold=2, recovery_time=60,
     ),
 )

	await relay._tick()  # row 1 fails -> failure 1 -> batch aborts
	assert _state_gauge('relay-test-trip') == 0.0
	await relay._tick()  # first unsent row fails again -> failure 2 -> open

	assert broker.sent == []
	assert _state_gauge('relay-test-trip') == 1.0


@pytest.mark.asyncio
async def test_relay_skips_batch_fails_fast_while_breaker_open():
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row() for _ in range(3)])
	broker = FakeBroker()  # Kafka actually healthy — breaker forced open
	breaker = CircuitBreaker(name='relay-test-skip', failure_threshold=5, recovery_time=60)
	breaker._open()
	rejected_before = _rejected('relay-test-skip')
	hist_before = _hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'}, sample='_count')
	relay = _build_relay(repo, broker, circuit_breaker=breaker)

	await relay._tick()

	# Fail fast: no doomed send attempted, rows stay unsent for a later tick.
	assert broker.sent == []
	remaining = await repo.get_unsent(10)
	assert len(remaining) == 3
	assert _rejected('relay-test-skip') - rejected_before == 1
	# A rejection is not an attempt: no new histogram sample (delta, not
	# absolute — earlier tests in this module share the global registry).
	assert (
		_hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'}, sample='_count')
		- hist_before
	) == 0


@pytest.mark.asyncio
async def test_relay_trips_mid_batch_and_skips_next_tick_without_histogram_sample():
	# Mid-batch trip, modeled the only way it happens in production: row 1 is
	# sent, then row 2's send FAILS hard — the failure trips the breaker
	# (threshold=1) and the batch aborts with row 1 marked sent. The NEXT tick
	# hits the open-state pre-check: rows skip without an attempt (no doomed
	# producer timeout, no histogram sample).
	#
	# NB: a trip raised *during a successful* operation would be wiped by
	# call()'s success-reset — an externally-set open state cannot survive a
	# success. That is fine here: the relay's 'kafka' breaker is private and
	# only ever trips through its own failed sends.
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row() for _ in range(3)])

	class FailOnSecondSendBroker(FakeBroker):
		async def send_message(self, key: bytes, topic: str, value: bytes) -> None:
			if len(self.sent) >= 1:  # row 2: Kafka goes down
				self.breaker._open()
				raise RuntimeError('kafka down mid-batch')
			await super().send_message(key, topic, value)

	breaker = CircuitBreaker(name='relay-test-race', failure_threshold=1, recovery_time=60)
	broker = FailOnSecondSendBroker()
	broker.breaker = breaker
	relay = _build_relay(repo, broker, circuit_breaker=breaker)

	# Tick 1: row 1 sent, row 2 fails -> trip -> batch aborts.
	await relay._tick()
	assert len(broker.sent) == 1
	assert _state_gauge('relay-test-race') == 1.0

	# Tick 2: pre-check skips without an attempt — fail fast.
	rejected_before = _rejected('relay-test-race')
	hist_before = _hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'}, sample='_count')
	await relay._tick()

	assert len(broker.sent) == 1  # no new sends
	assert _rejected('relay-test-race') - rejected_before == 1
	assert (
		_hist_value('outbox_publish_duration_seconds', {'topic': 'chat-events'}, sample='_count')
		- hist_before
	) == 0  # a rejection is not an attempt
	remaining = await repo.get_unsent(10)
	assert len(remaining) == 2


@pytest.mark.asyncio
async def test_relay_resumes_publishing_after_breaker_recovery():
	repo = MemoryOutboxRepository()
	row = _make_row()
	repo._outbox.append(row)
	broker = FakeBroker()
	breaker = CircuitBreaker(name='relay-test-recover', failure_threshold=1, recovery_time=60)
	relay = _build_relay(repo, broker, circuit_breaker=breaker)

	breaker._open()
	await relay._tick()
	assert broker.sent == []  # skipped while open

	# Simulate the recovery window elapsing -> half-open probe is allowed and
	# succeeds -> breaker closes -> publishing resumes.
	from time import monotonic
	breaker._opened_at = monotonic() - breaker.recovery_time - 1
	await relay._tick()

	assert len(broker.sent) == 1
	assert await repo.get_unsent(10) == []
	assert _state_gauge('relay-test-recover') == 0.0


@pytest.mark.asyncio
async def test_relay_without_breaker_behaves_as_before():
	# Back-compat guard: circuit_breaker=None (tests, dummy container) must
	# keep the legacy single-send-per-row path without breaker interactions.
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row(), _make_row()])
	broker = FakeBroker()
	relay = _build_relay(repo, broker, circuit_breaker=None)

	await relay._tick()

	assert len(broker.sent) == 2
	assert await repo.get_unsent(10) == []


@pytest.mark.asyncio
async def test_relay_outbox_pending_reflects_true_backlog_beyond_batch_size(metric_baselines):
	# G10 regression (ADR-0006 Chunk 2.1): the gauge must report the FULL unsent
	# backlog, not just the batch_size-capped snapshot from get_unsent().
	repo = MemoryOutboxRepository()
	repo._outbox.extend([_make_row() for _ in range(15)])
	broker = FakeBroker()
	relay = _build_relay(repo, broker)  # batch_size=10 < backlog of 15

	# First tick: sends the first batch of 10; 5 rows must remain pending.
	await relay._tick()

	assert len(broker.sent) == 10
	assert _value('outbox_pending') == 5

	# Second tick: drains the rest.
	await relay._tick()

	assert len(broker.sent) == 15
	assert _value('outbox_pending') == 0

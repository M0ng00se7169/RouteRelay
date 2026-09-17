from datetime import (
    datetime,
    timezone,
)
from uuid import uuid4

import pytest
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.outbox.base import OutboxRow
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.relay import OutboxRelay
from prometheus_client import REGISTRY


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
		occurred_at=datetime.now(timezone.utc),
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


def _build_relay(repo: MemoryOutboxRepository, broker: FakeBroker) -> OutboxRelay:
	return OutboxRelay(
		outbox_repository=repo,
		message_broker=broker,
		poll_interval=0.0,
		batch_size=10,
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

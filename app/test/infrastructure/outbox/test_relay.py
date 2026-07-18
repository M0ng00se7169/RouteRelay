from datetime import datetime, timezone
from uuid import uuid4

import pytest
from prometheus_client import REGISTRY

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.outbox.base import OutboxRow
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.relay import OutboxRelay

METRIC_NAMES = (
	'outbox_published_total',
	'outbox_publish_errors_total',
	'outbox_pending',
	'kafka_messages_sent_total',
)


def _value(name: str) -> float:
	value = REGISTRY.get_sample_value(name)
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
	return {name: _value(name) for name in METRIC_NAMES}


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
	assert _value('kafka_messages_sent_total') - metric_baselines['kafka_messages_sent_total'] == 2
	assert _value('outbox_pending') == 0


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
	assert _value('kafka_messages_sent_total') - metric_baselines['kafka_messages_sent_total'] == 0

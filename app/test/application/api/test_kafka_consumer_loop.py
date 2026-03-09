"""Unit tests for the instrumented Kafka consumer loop (ADR-0006, Chunks
3.1 + 3.2): per-message counters and the ``kafka_consumer_up`` heartbeat.

The loop is async and runs until cancelled, so tests drive it with a broker
whose `start_consuming` yields a fixed batch of messages and then blocks
forever; the loop task is cancelled once the metric assertions pass.
"""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from prometheus_client import REGISTRY

from application.api.lifespan import (
	_kafka_consumer_loop,
	start_kafka_consumer,
	stop_kafka_consumer,
)


TOPIC = 'new-messages'


def _counter_value(name: str, labels: dict | None = None) -> float:
	value = REGISTRY.get_sample_value(name, labels or {})
	return value if value is not None else 0.0


class _BatchBroker:
	"""Yields a fixed batch of messages, then blocks forever (like a real
	idle consumer) so the loop stays alive until the test cancels it."""

	def __init__(self, messages: list) -> None:
		self._messages = messages

	def start_consuming(self, topic: str):
		return self._consume()

	async def _consume(self):
		for message in self._messages:
			yield message
		await asyncio.Event().wait()  # block forever — never raises

	def stop_consuming(self):
		pass


@pytest.mark.asyncio
async def test_consumer_loop_counts_consumed_published_malformed():
	# 2 valid + 1 malformed → consumed == 3, published == 2, malformed == 1.
	messages = [
		{'chat_oid': 'c1', 'message_text': 'hello'},
		{'chat_oid': 'c2', 'message_text': 'world'},
		{'no_chat_oid': True},  # malformed — must be counted, not published
	]
	mediator = AsyncMock()
	mediator.publish = AsyncMock()

	consumed_before = _counter_value('kafka_messages_consumed_total', {'topic': TOPIC})
	published_before = _counter_value(
		'kafka_consumer_events_published_total', {'topic': TOPIC}
	)
	malformed_before = _counter_value('kafka_consumer_malformed_total')

	task = asyncio.create_task(
		_kafka_consumer_loop(_BatchBroker(messages), TOPIC, mediator)
	)

	for _ in range(100):
		if mediator.publish.await_count >= 2:
			break
		await asyncio.sleep(0.01)

	# Let the malformed row's counter update land too.
	await asyncio.sleep(0.05)

	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert mediator.publish.await_count == 2
	assert (
		_counter_value('kafka_messages_consumed_total', {'topic': TOPIC})
		- consumed_before
	) == 3
	assert (
		_counter_value('kafka_consumer_events_published_total', {'topic': TOPIC})
		- published_before
	) == 2
	assert (
		_counter_value('kafka_consumer_malformed_total') - malformed_before
	) == 1


@pytest.mark.asyncio
async def test_consumer_loop_counts_errors_without_dying():
	# A mediator failure must increment the error counter, log, and keep the
	# loop alive for the next message.
	messages = [
		{'chat_oid': 'c1', 'message_text': 'boom'},
		{'chat_oid': 'c2', 'message_text': 'ok'},
	]
	mediator = AsyncMock()
	mediator.publish = AsyncMock(side_effect=[RuntimeError('mediator down'), None])

	errors_before = _counter_value('kafka_consumer_errors_total', {'topic': TOPIC})
	published_before = _counter_value(
		'kafka_consumer_events_published_total', {'topic': TOPIC}
	)

	task = asyncio.create_task(
		_kafka_consumer_loop(_BatchBroker(messages), TOPIC, mediator)
	)

	for _ in range(100):
		if mediator.publish.await_count >= 2:
			break
		await asyncio.sleep(0.01)

	await asyncio.sleep(0.05)

	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert mediator.publish.await_count == 2
	assert (
		_counter_value('kafka_consumer_errors_total', {'topic': TOPIC})
		- errors_before
	) == 1
	assert (
		_counter_value('kafka_consumer_events_published_total', {'topic': TOPIC})
		- published_before
	) == 1


# --- kafka_consumer_up heartbeat (ADR-0006, Chunk 3.2) ----------------------


class _BlockingBroker:
	"""Consumer that yields nothing and blocks forever (healthy but idle)."""

	def start(self):
		pass

	def close(self):
		pass

	def start_consuming(self, topic: str):
		return self._consume()

	async def _consume(self):
		await asyncio.Event().wait()  # block forever — never raises

	def stop_consuming(self):
		pass


class _CrashingBroker(_BlockingBroker):
	"""Consumer whose iterator dies immediately with an exception."""

	async def _consume(self):
		raise RuntimeError('consumer crashed')
		yield  # pragma: no cover — makes this an async generator


class FakeContainer:
	def __init__(self, mapping):
		self._mapping = mapping

	def __call__(self):
		return self

	def resolve(self, cls):
		return self._mapping[cls]

	def register(self, service=None, factory=None, instance=None, scope=None, **kwargs):
		pass


def _app_with(mapping):
	from fastapi import FastAPI

	from logic.init import init_container

	app = FastAPI()
	app.dependency_overrides[init_container] = FakeContainer(mapping)
	return app


@pytest.mark.asyncio
async def test_consumer_heartbeat_transitions_on_start_and_stop():
	from infrastructure.message_brokers.base import BaseMessageBroker
	from logic.mediator.base import Mediator
	from settings.config import Config

	broker = _BlockingBroker()
	app = _app_with({
		BaseMessageBroker: broker,
		Mediator: AsyncMock(),
		Config: SimpleNamespace(new_message_received_topic=TOPIC),
	})

	up_before = _counter_value('kafka_consumer_up')

	task = await start_kafka_consumer(app)
	# add_done_callback is invoked synchronously on completion; for a healthy
	# start it must not have touched the gauge.
	assert _counter_value('kafka_consumer_up') - up_before == 1

	await stop_kafka_consumer(task, app)

	assert _counter_value('kafka_consumer_up') - up_before == 0


@pytest.mark.asyncio
async def test_consumer_heartbeat_drops_on_crash():
	from infrastructure.message_brokers.base import BaseMessageBroker
	from logic.mediator.base import Mediator
	from settings.config import Config

	app = _app_with({
		BaseMessageBroker: _CrashingBroker(),
		Mediator: AsyncMock(),
		Config: SimpleNamespace(new_message_received_topic=TOPIC),
	})

	up_before = _counter_value('kafka_consumer_up')

	task = await start_kafka_consumer(app)
	assert _counter_value('kafka_consumer_up') - up_before == 1

	# The loop dies on its own; the done callback must clear the heartbeat.
	with pytest.raises(RuntimeError):
		await task

	assert _counter_value('kafka_consumer_up') - up_before == 0

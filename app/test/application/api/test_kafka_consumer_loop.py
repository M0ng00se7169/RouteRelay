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
    _drop_consumer_heartbeat_if_dead,
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
		'kafka_consumer_events_published_total', {'topic': TOPIC},
	)
	malformed_before = _counter_value('kafka_consumer_malformed_total')

	task = asyncio.create_task(
		_kafka_consumer_loop(_BatchBroker(messages), TOPIC, mediator),
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
		'kafka_consumer_events_published_total', {'topic': TOPIC},
	)

	task = asyncio.create_task(
		_kafka_consumer_loop(_BatchBroker(messages), TOPIC, mediator),
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


# --- reconnect / backoff helpers (O-1) ---------------------------------------


async def _crash_stream():
	raise RuntimeError('kafka down')
	yield  # pragma: no cover — makes this an async generator


async def _msg_then_crash_stream(text: str):
	yield {'chat_oid': 'c1', 'message_text': text}
	raise RuntimeError('kafka down after message')


async def _msg_then_clean_exit_stream(text: str):
	yield {'chat_oid': 'c1', 'message_text': text}
	# Clean return — the O-1 case: no exception, but the stream ended.


async def _msg_then_block_stream(text: str):
	yield {'chat_oid': 'c1', 'message_text': text}
	await asyncio.Event().wait()  # block forever — never raises


class _ScriptedBroker:
	"""`start_consuming` returns the scripted streams in order; once the script
	is exhausted it blocks forever (healthy idle consumer) and sets `exhausted`
	so tests can await that instead of polling with asyncio.sleep (which would
	interleave with the monkeypatched sleep under measurement)."""

	def __init__(self, streams: list) -> None:
		self._streams = list(streams)
		self.calls = 0
		self.exhausted = asyncio.Event()

	def start_consuming(self, topic: str):
		self.calls += 1
		if self._streams:
			stream = self._streams.pop(0)
			if not self._streams:
				# The last stream always blocks forever by construction, so no
				# further start_consuming call would ever fire the event.
				self.exhausted.set()
			return stream()
		self.exhausted.set()
		return self._block()

	async def _block(self):
		await asyncio.Event().wait()
		yield  # pragma: no cover — unreachable, keeps this an async generator


def _fake_config() -> SimpleNamespace:
	return SimpleNamespace(
		new_message_received_topic=TOPIC,
		kafka_consumer_backoff_initial=0.01,
		kafka_consumer_backoff_max=0.05,
	)


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
		Config: _fake_config(),
	})

	up_before = _counter_value('kafka_consumer_up')

	task = await start_kafka_consumer(app)
	# add_done_callback is invoked synchronously on completion; for a healthy
	# start it must not have touched the gauge.
	assert _counter_value('kafka_consumer_up') - up_before == 1

	await stop_kafka_consumer(task, app)

	assert _counter_value('kafka_consumer_up') - up_before == 0


@pytest.mark.asyncio
async def test_consumer_loop_survives_crash_and_reconnects():
	# O-1: a stream crash must NOT kill the loop. The task stays alive, counts
	# reconnects, and keeps the heartbeat up; only stop_kafka_consumer drops it.
	from infrastructure.message_brokers.base import BaseMessageBroker

	from logic.mediator.base import Mediator
	from settings.config import Config

	app = _app_with({
		BaseMessageBroker: _CrashingBroker(),
		Mediator: AsyncMock(),
		Config: _fake_config(),
	})

	up_before = _counter_value('kafka_consumer_up')
	reconnects_before = _counter_value('kafka_consumer_reconnects_total', {'topic': TOPIC})

	task = await start_kafka_consumer(app)
	assert _counter_value('kafka_consumer_up') - up_before == 1

	# Give the loop time to crash and reconnect several times.
	await asyncio.sleep(0.1)

	assert _counter_value('kafka_consumer_up') - up_before == 1
	assert (
		_counter_value('kafka_consumer_reconnects_total', {'topic': TOPIC})
		- reconnects_before
	) >= 2

	await stop_kafka_consumer(task, app)
	assert _counter_value('kafka_consumer_up') - up_before == 0


@pytest.mark.asyncio
async def test_heartbeat_drops_on_clean_task_exit():
	# O-1: the done-callback must clear the heartbeat for ANY non-cancelled
	# completion — including a clean return. The old code dropped it only on
	# exceptions, so a cleanly-exited loop looked "up" while consuming nothing.
	from infrastructure.metrics import (
	    kafka_consumer_up,
	    safe_set,
	)

	safe_set(kafka_consumer_up, 1)

	async def _returns_cleanly() -> None:
		return None

	task = asyncio.create_task(_returns_cleanly())
	task.add_done_callback(_drop_consumer_heartbeat_if_dead)
	await task

	assert _counter_value('kafka_consumer_up') == 0.0


@pytest.mark.asyncio
async def test_consumer_loop_reconnects_after_clean_stream_exit():
	# O-1: a clean iterator exit must trigger a reconnect — the second stream
	# (a fresh start_consuming call) delivers another message.
	mediator = AsyncMock()
	reconnects_before = _counter_value('kafka_consumer_reconnects_total', {'topic': TOPIC})

	broker = _ScriptedBroker([
		lambda: _msg_then_clean_exit_stream('first'),
		lambda: _msg_then_block_stream('second'),
	])

	task = asyncio.create_task(
		_kafka_consumer_loop(broker, TOPIC, mediator, backoff_initial=0.01, backoff_max=0.05),
	)

	for _ in range(200):
		if mediator.publish.await_count >= 2:
			break
		await asyncio.sleep(0.01)

	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert mediator.publish.await_count == 2
	assert broker.calls == 2
	assert (
		_counter_value('kafka_consumer_reconnects_total', {'topic': TOPIC})
		- reconnects_before
	) == 1


@pytest.mark.asyncio
async def test_backoff_grows_and_caps(monkeypatch):
	# Successive reconnects double the delay up to backoff_max. asyncio.sleep is
	# spied (delegating to the real sleep, capped) to capture the delays.
	real_sleep = asyncio.sleep
	delays: list[float] = []

	async def spy_sleep(delay, *args, **kwargs):
		delays.append(delay)
		await real_sleep(min(delay, 0.01))

	monkeypatch.setattr('application.api.lifespan.asyncio.sleep', spy_sleep)

	broker = _ScriptedBroker([
		lambda: _crash_stream(),
		lambda: _crash_stream(),
		lambda: _crash_stream(),
		lambda: _crash_stream(),
		lambda: _msg_then_block_stream('ok'),
	])

	task = asyncio.create_task(
		_kafka_consumer_loop(broker, TOPIC, AsyncMock(), backoff_initial=0.1, backoff_max=0.3),
	)

	# All backoff sleeps are recorded by the time the script is exhausted.
	await asyncio.wait_for(broker.exhausted.wait(), timeout=2)

	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert delays == [0.1, 0.2, 0.3, 0.3]


@pytest.mark.asyncio
async def test_backoff_resets_after_successful_message(monkeypatch):
	# Receiving a message resets the backoff: crash (sleep 0.1) → message (reset)
	# → crash again must sleep 0.1 again, NOT the doubled 0.2.
	real_sleep = asyncio.sleep
	delays: list[float] = []

	async def spy_sleep(delay, *args, **kwargs):
		delays.append(delay)
		await real_sleep(min(delay, 0.01))

	monkeypatch.setattr('application.api.lifespan.asyncio.sleep', spy_sleep)

	broker = _ScriptedBroker([
		lambda: _crash_stream(),
		lambda: _msg_then_crash_stream('boom'),
		lambda: _msg_then_block_stream('ok'),
	])

	task = asyncio.create_task(
		_kafka_consumer_loop(broker, TOPIC, AsyncMock(), backoff_initial=0.1, backoff_max=5.0),
	)

	await asyncio.wait_for(broker.exhausted.wait(), timeout=2)

	task.cancel()
	try:
		await task
	except asyncio.CancelledError:
		pass

	assert delays == [0.1, 0.1]

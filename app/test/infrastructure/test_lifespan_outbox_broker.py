import asyncio
from contextlib import asynccontextmanager
from unittest.mock import (
    AsyncMock,
    MagicMock,
)

from fastapi import FastAPI

import pytest
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.message_brokers.kafka import KafkaMessageBroker
from infrastructure.outbox.mapper import resolve_topic
from infrastructure.outbox.mongo import MongoOutboxRepository
from infrastructure.outbox.relay import OutboxRelay
from infrastructure.outbox.session import MongoSessionProvider

from application.api.lifespan import (
    close_message_broker,
    init_message_broker,
    start_relay,
    stop_relay,
)
from domain.events.messages import (
    ChatDeletedEvent,
    NewChatCreatedEvent,
    NewMessageReceivedEvent,
    NewMessageReceivedFromBrokerEvent,
)
from logic.events.messages import (
    ChatDeletedEventHandler,
    NewChatCreatedEventHandler,
    NewMessageReceivedEventHandler,
    NewMessageReceivedFromBrokerEventHandler,
)
from logic.init import init_container


class FakeContainer:
	def __init__(self, mapping):
		self._mapping = mapping

	def __call__(self):
		return self

	def resolve(self, cls):
		return self._mapping[cls]

	def register(self, service=None, factory=None, instance=None, scope=None, **kwargs):
		pass


# --- lifespan glue ---------------------------------------------------------


def _app_with(container):
	app = FastAPI()
	app.dependency_overrides = {init_container: container}
	return app


@pytest.mark.asyncio
async def test_init_message_broker_starts_broker():
	broker = AsyncMock(spec=BaseMessageBroker)
	container = FakeContainer({BaseMessageBroker: broker})
	app = _app_with(container)

	await init_message_broker(app)

	broker.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_message_broker_closes_broker():
	broker = AsyncMock(spec=BaseMessageBroker)
	container = FakeContainer({BaseMessageBroker: broker})
	app = _app_with(container)

	await close_message_broker(app)

	broker.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_start_relay_returns_task():
	relay = AsyncMock()
	relay.run = AsyncMock()
	container = FakeContainer({OutboxRelay: relay})
	app = _app_with(container)

	task = await start_relay(app)

	assert isinstance(task, asyncio.Task)
	task.cancel()


@pytest.mark.asyncio
async def test_stop_relay_cancels_task():
	relay = AsyncMock()
	relay.run = AsyncMock()
	container = FakeContainer({OutboxRelay: relay})
	app = _app_with(container)

	task = await start_relay(app)
	await stop_relay(task)

	assert task.cancelled()


@pytest.mark.asyncio
async def test_stop_relay_handles_none():
	# None should be a no-op (no exception raised).
	await stop_relay(None)


@pytest.mark.asyncio
async def test_lifespan_runs_all_phases():
	broker = AsyncMock(spec=BaseMessageBroker)
	relay = AsyncMock()
	relay.run = AsyncMock()
	container = FakeContainer({BaseMessageBroker: broker, OutboxRelay: relay})

	@asynccontextmanager
	async def fake_lifespan(app):
		await init_message_broker(app)
		started = await start_relay(app)
		try:
			yield
		finally:
			await stop_relay(started)
			await close_message_broker(app)

	from logic.init import init_container
	app = FastAPI()
	app.dependency_overrides = {init_container: container}

	async with fake_lifespan(app):
		pass

	broker.start.assert_awaited_once()
	broker.close.assert_awaited_once()


# --- Kafka broker ----------------------------------------------------------


@pytest.mark.asyncio
async def test_kafka_start_creates_producer_and_consumer():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')

	producer = AsyncMock()
	consumer = AsyncMock()
	# Patch the aiokafka classes so start() does not open a real network connection.
	import infrastructure.message_brokers.kafka as kafka_mod
	orig_p, orig_c = kafka_mod.AIOKafkaProducer, kafka_mod.AIOKafkaConsumer
	kafka_mod.AIOKafkaProducer = lambda **k: producer  # noqa: N801
	kafka_mod.AIOKafkaConsumer = lambda **k: consumer  # noqa: N801
	try:
		await broker.start()
	finally:
		kafka_mod.AIOKafkaProducer, kafka_mod.AIOKafkaConsumer = orig_p, orig_c

	assert broker.producer is producer
	assert broker.consumer is consumer
	producer.start.assert_awaited_once()
	consumer.start.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_send_message_uses_producer():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	producer = AsyncMock()
	broker.producer = producer

	await broker.send_message(key=b'k', topic='t', value=b'msg')

	producer.send.assert_awaited_once_with(topic='t', key=b'k', value=b'msg')


@pytest.mark.asyncio
async def test_kafka_send_message_before_start_raises():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	broker.producer = None

	with pytest.raises(RuntimeError):
		await broker.send_message(key=b'k', topic='t', value=b'msg')


@pytest.mark.asyncio
async def test_kafka_close_stops_producer_and_consumer():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	producer = AsyncMock()
	consumer = AsyncMock()
	broker.producer = producer
	broker.consumer = consumer

	await broker.close()

	producer.stop.assert_awaited_once()
	consumer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_close_no_producer_no_consumer():
	# close() must not raise when nothing was started.
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	broker.producer = None
	broker.consumer = None

	await broker.close()


@pytest.mark.asyncio
async def test_kafka_start_consuming_before_start_raises():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	broker.consumer = None

	with pytest.raises(RuntimeError):
		async for _ in broker.start_consuming(topic='t'):
			pass


@pytest.mark.asyncio
async def test_kafka_start_consuming_yields_messages():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	consumer = AsyncMock()
	broker.consumer = consumer

	from orjson import dumps

	msgs = [MagicMock(value=dumps({'a': 1})), MagicMock(value=dumps({'a': 2}))]

	async def _gen():
		for m in msgs:
			yield m

	consumer.__aiter__ = MagicMock(return_value=_gen())
	consumer.subscribe = MagicMock()

	got = []
	async for item in broker.start_consuming(topic='t'):
		got.append(item)

	assert got == [{'a': 1}, {'a': 2}]
	consumer.subscribe.assert_called_once_with(topics=['t'])


@pytest.mark.asyncio
async def test_kafka_stop_consuming_unsubscribes():
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	consumer = AsyncMock()
	consumer.unsubscribe = MagicMock()
	broker.consumer = consumer

	await broker.stop_consuming()

	consumer.unsubscribe.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_stop_consuming_no_consumer():
	# stop_consuming() must be a no-op when no consumer exists.
	broker = KafkaMessageBroker(bootstrap_servers='localhost:9092')
	broker.consumer = None

	await broker.stop_consuming()


# --- Outbox mapper ---------------------------------------------------------


def test_resolve_topic_known_events():
	assert resolve_topic(NewChatCreatedEvent(chat_oid='c', chat_title='t')) == 'new-chats-topic'
	assert resolve_topic(NewMessageReceivedEvent(message_text='m', message_oid='m1', chat_oid='c')) == 'new-messages'
	assert resolve_topic(ChatDeletedEvent(chat_oid='c')) == 'chat-deleted-topic'
	assert resolve_topic(NewChatCreatedEvent(chat_oid='c', chat_title='t')) == 'new-chats-topic'


def test_resolve_topic_unknown_raises():
	class UnknownEvent:
		event_id = 'x'
		occurred_at = None

	with pytest.raises(ValueError):
		resolve_topic(UnknownEvent())  # type: ignore[arg-type]


# --- Outbox mongo ----------------------------------------------------------


class _Cursor:
	def __init__(self, docs):
		self._docs = docs

	def limit(self, n):
		self._docs = self._docs[:n]
		return self

	async def __aiter__(self):
		for d in self._docs:
			yield d


@pytest.mark.asyncio
async def test_mongo_outbox_save_events():
	collection = MagicMock()
	collection.insert_many = AsyncMock()
	resolver = MagicMock(return_value='t')
	repo = MongoOutboxRepository(collection=collection, _topic_resolver=resolver)

	event = NewMessageReceivedEvent(message_text='m', message_oid='m1', chat_oid='c')

	await repo.save_events([event], session=None)

	collection.insert_many.assert_awaited_once()
	resolver.assert_called_once_with(event)


@pytest.mark.asyncio
async def test_mongo_outbox_save_events_empty():
	collection = MagicMock()
	collection.insert_many = AsyncMock()
	repo = MongoOutboxRepository(collection=collection, _topic_resolver=MagicMock())

	await repo.save_events([], session=None)

	collection.insert_many.assert_not_called()


@pytest.mark.asyncio
async def test_mongo_outbox_get_unsent():
	collection = MagicMock()
	doc = {'_id': 'x', 'event_id': 'e', 'topic': 't', 'key': b'k', 'payload': b'p', 'occurred_at': None, 'sent': False, 'sent_at': None}
	collection.find = MagicMock(return_value=_Cursor([doc]))
	repo = MongoOutboxRepository(collection=collection, _topic_resolver=MagicMock())

	rows = await repo.get_unsent(limit=10)

	assert len(rows) == 1
	assert rows[0]._id == 'x'
	assert rows[0].topic == 't'


@pytest.mark.asyncio
async def test_mongo_outbox_mark_as_sent():
	collection = MagicMock()
	collection.update_many = AsyncMock()
	repo = MongoOutboxRepository(collection=collection, _topic_resolver=MagicMock())

	await repo.mark_as_sent(['x', 'y'])

	collection.update_many.assert_awaited_once()


@pytest.mark.asyncio
async def test_mongo_outbox_count_unsent():
	collection = MagicMock()
	collection.count_documents = AsyncMock(return_value=7)
	repo = MongoOutboxRepository(collection=collection, _topic_resolver=MagicMock())

	count = await repo.count_unsent()

	assert count == 7
	collection.count_documents.assert_awaited_once_with({'sent': False})


# --- Relay ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_relay_tick_publishes_and_marks_sent():
	rows = [
		MagicMock(_id='r1', topic='t', key=b'k', payload=b'p'),
		MagicMock(_id='r2', topic='t', key=b'k', payload=b'p'),
	]
	repo = AsyncMock()
	repo.get_unsent = AsyncMock(return_value=rows)
	repo.count_unsent = AsyncMock(return_value=0)
	repo.mark_as_sent = AsyncMock()
	broker = AsyncMock(spec=BaseMessageBroker)
	relay = OutboxRelay(outbox_repository=repo, message_broker=broker)

	await relay._tick()

	broker.send_message.assert_awaited()
	assert broker.send_message.await_count == 2
	repo.mark_as_sent.assert_awaited_once_with(['r1', 'r2'])


@pytest.mark.asyncio
async def test_relay_tick_no_rows():
	repo = AsyncMock()
	repo.get_unsent = AsyncMock(return_value=[])
	repo.count_unsent = AsyncMock(return_value=0)
	broker = AsyncMock(spec=BaseMessageBroker)
	relay = OutboxRelay(outbox_repository=repo, message_broker=broker)

	await relay._tick()

	broker.send_message.assert_not_called()
	repo.mark_as_sent.assert_not_called()


@pytest.mark.asyncio
async def test_relay_tick_stops_on_error():
	rows = [
		MagicMock(_id='r1', topic='t', key=b'k', payload=b'p'),
		MagicMock(_id='r2', topic='t', key=b'k', payload=b'p'),
	]
	repo = AsyncMock()
	repo.get_unsent = AsyncMock(return_value=rows)
	repo.count_unsent = AsyncMock(return_value=2)
	repo.mark_as_sent = AsyncMock()
	broker = AsyncMock(spec=BaseMessageBroker)
	broker.send_message.side_effect = RuntimeError('kafka down')
	relay = OutboxRelay(outbox_repository=repo, message_broker=broker)

	await relay._tick()

	# Only the first row attempt; second is skipped after the failure.
	assert broker.send_message.await_count == 1
	repo.mark_as_sent.assert_not_called()


@pytest.mark.asyncio
async def test_relay_run_loops_until_cancelled():
	repo = AsyncMock()
	repo.get_unsent = AsyncMock(return_value=[])
	repo.count_unsent = AsyncMock(return_value=0)
	broker = AsyncMock(spec=BaseMessageBroker)
	relay = OutboxRelay(outbox_repository=repo, message_broker=broker, poll_interval=0.01)

	task = asyncio.create_task(relay.run())
	await asyncio.sleep(0.05)
	task.cancel()
	with pytest.raises(asyncio.CancelledError):
		await task


def test_build_relay_uses_config():
	from infrastructure.outbox.relay import build_relay

	repo = AsyncMock()
	broker = AsyncMock(spec=BaseMessageBroker)
	config = MagicMock()
	config.outbox_relay_poll_interval = 5.0

	relay = build_relay(outbox_repository=repo, message_broker=broker, config=config)

	assert relay.poll_interval == 5.0
	assert relay.outbox_repository is repo
	assert relay.message_broker is broker


# --- Events handlers -------------------------------------------------------


@pytest.mark.asyncio
async def test_new_chat_created_handler_is_noop():
	handler = NewChatCreatedEventHandler(message_broker=AsyncMock(), connection_manager=AsyncMock())
	event = NewChatCreatedEvent(chat_oid='c', chat_title='t')

	await handler.handle(event)  # should not raise


@pytest.mark.asyncio
async def test_new_message_received_handler_is_noop():
	handler = NewMessageReceivedEventHandler(message_broker=AsyncMock(), connection_manager=AsyncMock())
	event = NewMessageReceivedEvent(message_text='m', message_oid='m1', chat_oid='c')

	await handler.handle(event)  # should not raise


@pytest.mark.asyncio
async def test_chat_deleted_handler_disconnects_all():
	manager = AsyncMock()
	handler = ChatDeletedEventHandler(message_broker=AsyncMock(), connection_manager=manager)
	event = ChatDeletedEvent(chat_oid='c1')

	await handler.handle(event)

	manager.disconnect_all.assert_awaited_once_with(key='c1')


@pytest.mark.asyncio
async def test_new_message_from_broker_handler():
	manager = AsyncMock()
	handler = NewMessageReceivedFromBrokerEventHandler(connection_manager=manager, message_broker=AsyncMock())
	event = NewMessageReceivedFromBrokerEvent(message='hi', chat_oid='c1')

	await handler.handle(event)

	manager.send_all.assert_awaited_once_with(key='c1', bytes_=b'hi')


# --- MongoSessionProvider --------------------------------------------------


@pytest.mark.asyncio
async def test_mongo_session_provider_returns_session():
	client = AsyncMock()
	client.start_session = AsyncMock(return_value='session-obj')
	provider = MongoSessionProvider(client=client)

	session = await provider()

	assert session == 'session-obj'
	client.start_session.assert_awaited_once()

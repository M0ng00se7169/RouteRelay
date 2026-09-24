import asyncio
import logging

from fastapi import FastAPI

from httpx import AsyncClient
from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.metrics import (
    kafka_consumer_errors_total,
    kafka_consumer_events_published_total,
    kafka_consumer_malformed_total,
    kafka_consumer_up,
    kafka_messages_consumed_total,
    safe_inc,
    safe_set,
)
from infrastructure.outbox.relay import OutboxRelay
from punq import Scope

from domain.events.messages import NewMessageReceivedFromBrokerEvent
from logic.init import init_container
from logic.mediator.base import Mediator
from settings.config import Config


logger = logging.getLogger(__name__)





def close_http_client(client: 'AsyncClient') -> None:
    asyncio.create_task(client.aclose())


def _active_container(app: FastAPI | None = None):
    # Tests install `init_container` in `app.dependency_overrides` (see
    # `app/test/application/api/conftest.py`) to swap Mongo/Kafka for in-memory
    # doubles. Honour that override here so the broker + relay started in the
    # lifespan also use the test doubles — otherwise the relay would resolve the
    # real Kafka broker and the production Mongo outbox repository.
    if app is not None and init_container in app.dependency_overrides:
        return app.dependency_overrides[init_container]()
    return init_container()


async def init_message_broker(app: FastAPI | None = None):
    container = _active_container(app)
    message_broker: BaseMessageBroker = container.resolve(BaseMessageBroker)
    await message_broker.start()

    def init_http_client_factory():
        return AsyncClient()

    container.register(AsyncClient, factory=init_http_client_factory, scope=Scope.singleton)

    # Register the Telegram Notification Client as a singleton
    from infrastructure.integrations.notifications.clients.telegram import TelegramNotificationClient
    container.register(TelegramNotificationClient, scope=Scope.singleton)


async def close_message_broker(app: FastAPI | None = None):
    container = _active_container(app)
    message_broker: BaseMessageBroker = container.resolve(BaseMessageBroker)
    await message_broker.close()


async def start_relay(app: FastAPI | None = None) -> asyncio.Task:
    container = _active_container(app)
    relay: OutboxRelay = container.resolve(OutboxRelay)
    return asyncio.create_task(relay.run(), name='outbox-relay')


async def stop_relay(task: asyncio.Task) -> None:
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


async def _kafka_consumer_loop(broker: BaseMessageBroker, topic: str, mediator) -> None:
	consumer = broker.start_consuming(topic)
	async for message in consumer:
		# Count every received message before parsing (ADR-0006, Chunk 3.1).
		safe_inc(kafka_messages_consumed_total, topic=topic)
		try:
			# Wire contract: the outbox relay serializes NewMessageReceivedEvent
			# via convert_event_to_broker_message, whose field is `message_text`
			# (the old `message` key never existed in real payloads — every
			# message was miscounted as malformed until the smoke test caught it).
			chat_oid = message.get('chat_oid')
			message_text = message.get('message_text')

			if chat_oid and message_text:
				event = NewMessageReceivedFromBrokerEvent(
					message=message_text,
					chat_oid=chat_oid,
				)
				# publish() expects an iterable of events.
				await mediator.publish([event])
				safe_inc(kafka_consumer_events_published_total, topic=topic)
			else:
				# Malformed payload (missing/empty chat_oid or message): has no
				# reliable identity, so it is counted, not published.
				safe_inc(kafka_consumer_malformed_total)
		except Exception:
			logger.exception('Error processing Kafka message')
			safe_inc(kafka_consumer_errors_total, topic=topic)


async def start_kafka_consumer(app: FastAPI | None = None) -> asyncio.Task:
    container = _active_container(app)
    broker: BaseMessageBroker = container.resolve(BaseMessageBroker)
    mediator = container.resolve(Mediator)
    config: Config = container.resolve(Config)

    # Liveness heartbeat (ADR-0006, Chunk 3.2): alert candidate — if this is 0
    # while the process is running, inbound events are not being consumed.
    safe_set(kafka_consumer_up, 1)
    task = asyncio.create_task(
        _kafka_consumer_loop(broker, config.new_message_received_topic, mediator),
        name='kafka-consumer',
    )
    # If the loop dies unexpectedly (crash instead of graceful stop), the
    # heartbeat must drop so the alert catches it.
    task.add_done_callback(_drop_consumer_heartbeat_if_dead)

    return task


def _drop_consumer_heartbeat_if_dead(task: asyncio.Task) -> None:
    # Graceful shutdown paths (stop_kafka_consumer / lifespan teardown) cancel
    # the task; only an *unexpected* termination must clear the heartbeat here.
    if not task.cancelled() and task.exception() is not None:
        safe_set(kafka_consumer_up, 0)


async def stop_kafka_consumer(task: asyncio.Task, app: FastAPI | None = None) -> None:
    safe_set(kafka_consumer_up, 0)
    if task is None:
        return
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    container = _active_container(app)
    broker: BaseMessageBroker = container.resolve(BaseMessageBroker)
    broker.stop_consuming()


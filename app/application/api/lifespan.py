import asyncio

from fastapi import FastAPI

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.outbox.relay import OutboxRelay

from logic.init import init_container


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


from collections.abc import AsyncIterator
from dataclasses import (
	dataclass,
	field,
)
from typing import Any

import orjson
from aiokafka import AIOKafkaConsumer
from aiokafka.producer import AIOKafkaProducer

from infrastructure.message_brokers.base import BaseMessageBroker


@dataclass
class KafkaMessageBroker(BaseMessageBroker):
	# The aiokafka producer/consumer are created lazily inside start() so that
	# constructing the broker does NOT require a running event loop. The previous
	# implementation built them in __init__, which calls get_running_loop() and
	# therefore fails when the broker is resolved off-loop (e.g. in test worker
	# threads, or at DI container build time).
	bootstrap_servers: str
	group_id: str = 'chat'
	metadata_max_age_ms: int = 30000
	# Durability: 'all' makes the awaited ack (send_and_wait) require every
	# in-sync replica, not just the leader — aiokafka defaults to acks=1. On the
	# single-broker dev cluster the ISR is just the leader, so behavior is
	# unchanged there; this hardens the guarantee for any cluster with RF>1.
	acks: str = 'all'
	# AIOKafkaProducer/AIOKafkaConsumer instances (created in start()). Annotated
	# as Any: aiokafka ships no type stubs, and strict mypy (disallow_any_unimported)
	# forbids leaking its untyped classes into annotations — including the
	# synthesized dataclass methods. All attribute access on them is untyped anyway.
	producer: Any = field(default=None, init=False)
	consumer: Any = field(default=None, init=False)

	async def start(self) -> None:
		self.producer = AIOKafkaProducer(
			bootstrap_servers=self.bootstrap_servers,
			acks=self.acks,
		)
		self.consumer = AIOKafkaConsumer(
			bootstrap_servers=self.bootstrap_servers,
			group_id=self.group_id,
			metadata_max_age_ms=self.metadata_max_age_ms,
		)
		await self.producer.start()
		await self.consumer.start()

	async def send_message(self, topic: str, key: bytes, value: bytes) -> None:
		if self.producer is None:
			raise RuntimeError('KafkaMessageBroker.send_message called before start()')
		# send() only buffers and returns a delivery future; awaiting THAT future
		# is what surfaces broker failures (KafkaTimeoutError etc.). Without it a
		# dead broker looks healthy: rows get marked sent, delivery errors vanish,
		# buffered rows die with the process (at-most-once). send_and_wait awaits
		# the future, restoring the relay's documented at-least-once contract and
		# giving the O-2 circuit breaker real failures to count.
		await self.producer.send_and_wait(topic=topic, key=key, value=value)

	async def close(self) -> None:
		if self.producer is not None:
			await self.producer.stop()
		if self.consumer is not None:
			await self.consumer.stop()

	def start_consuming(self, topic: str) -> AsyncIterator[dict[str, Any]]:
		if self.consumer is None:
			raise RuntimeError('KafkaMessageBroker.start_consuming called before start()')
		self.consumer.subscribe(topics=[topic])
		return self._consume()

	async def _consume(self) -> AsyncIterator[dict[str, Any]]:
		# mypy narrows `self.consumer` to None inside closures; assert it here.
		assert self.consumer is not None
		async for message in self.consumer:
			yield orjson.loads(message.value)

	def stop_consuming(self) -> None:
		if self.consumer is not None:
			self.consumer.unsubscribe()

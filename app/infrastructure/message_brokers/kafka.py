from dataclasses import (
    dataclass,
    field,
)
from typing import (
    AsyncIterator,
    Optional,
)

import orjson as orjson
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
	producer: Optional[AIOKafkaProducer] = field(default=None, init=False)
	consumer: Optional[AIOKafkaConsumer] = field(default=None, init=False)

	async def start(self):
		self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
		self.consumer = AIOKafkaConsumer(
			bootstrap_servers=self.bootstrap_servers,
			group_id=self.group_id,
			metadata_max_age_ms=self.metadata_max_age_ms,
		)
		await self.producer.start()
		await self.consumer.start()

	async def send_message(self, topic: str, key: bytes, value: bytes):
		if self.producer is None:
			raise RuntimeError('KafkaMessageBroker.send_message called before start()')
		# send() only buffers and returns a delivery future; awaiting THAT future
		# is what surfaces broker failures (KafkaTimeoutError etc.). Without it a
		# dead broker looks healthy: rows get marked sent, delivery errors vanish,
		# buffered rows die with the process (at-most-once). send_and_wait awaits
		# the future, restoring the relay's documented at-least-once contract and
		# giving the O-2 circuit breaker real failures to count.
		await self.producer.send_and_wait(topic=topic, key=key, value=value)

	async def close(self):
		if self.producer is not None:
			await self.producer.stop()
		if self.consumer is not None:
			await self.consumer.stop()

	async def start_consuming(self, topic: str) -> AsyncIterator[dict]:
		if self.consumer is None:
			raise RuntimeError('KafkaMessageBroker.start_consuming called before start()')
		self.consumer.subscribe(topics=[topic])

		async for message in self.consumer:
			yield orjson.loads(message.value)

	async def stop_consuming(self):
		if self.consumer is not None:
			self.consumer.unsubscribe()

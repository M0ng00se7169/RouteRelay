import asyncio

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.message_brokers.kafka import KafkaMessageBroker
from logic.init import init_container


async def start_kafka():
    container = init_container()
    message_broker: KafkaMessageBroker = container.resolve(BaseMessageBroker)
    await asyncio.sleep(10)
    await message_broker.producer.start()


async def stop_kafka():
    container = init_container()
    message_broker: KafkaMessageBroker = container.resolve(BaseMessageBroker)
    await message_broker.producer.stop()

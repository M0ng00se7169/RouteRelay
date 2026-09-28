from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any


@dataclass
class BaseMessageBroker(ABC):

    @abstractmethod
    async def start(self) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...

    @abstractmethod
    async def send_message(self, topic: str, key: bytes, value: bytes) -> None:
        ...

    # start_consuming returns the stream object synchronously (an async
    # generator); the consumer loop iterates it with `async for`. Declaring it
    # `async def` in the ABC never matched the async-generator implementations.
    @abstractmethod
    def start_consuming(self, topic: str) -> AsyncIterator[dict[str, Any]]:
        ...

    @abstractmethod
    def stop_consuming(self) -> None:
        ...

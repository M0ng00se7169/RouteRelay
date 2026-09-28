from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass
from typing import Any

from motor.motor_asyncio import (
    AsyncIOMotorClient,
    AsyncIOMotorClientSession,
)


class SessionProvider(ABC):
	@abstractmethod
	async def __call__(self) -> AsyncIOMotorClientSession | None:
		...


@dataclass
class MongoSessionProvider(SessionProvider):
	client: AsyncIOMotorClient[dict[str, Any]]

	async def __call__(self) -> AsyncIOMotorClientSession | None:
		return await self.client.start_session()

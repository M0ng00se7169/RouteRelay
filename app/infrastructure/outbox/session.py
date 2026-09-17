from abc import (
    ABC,
    abstractmethod,
)
from dataclasses import dataclass

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
	client: AsyncIOMotorClient

	async def __call__(self) -> AsyncIOMotorClientSession:
		return await self.client.start_session()

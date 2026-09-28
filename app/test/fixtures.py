import asyncio
from collections.abc import AsyncIterator

from punq import (
		Container,
		Scope,
)

from infrastructure.message_brokers.base import BaseMessageBroker
from infrastructure.outbox.base import BaseOutboxRepository
from infrastructure.outbox.memory import MemoryOutboxRepository
from infrastructure.outbox.relay import OutboxRelay
from infrastructure.outbox.session import SessionProvider
from infrastructure.repositories.messages.base import (
	BaseChatsRepository,
	BaseMessagesRepository,
)
from infrastructure.repositories.messages.memory import (
	MemoryChatRepository,
	MemoryMessagesRepository,
)
from infrastructure.resilience import (
	CircuitBreaker,
	CircuitBreakerChatsRepository,
	CircuitBreakerMessagesRepository,
)
from logic.init import (
	build_mediator,
	init_container,
)
from logic.mediator.base import Mediator
from logic.mediator.event import EventMediator
from settings.config import Config


class _NullSessionProvider(SessionProvider):
	async def __call__(self) -> None:
		return None


class _NoopOutboxRelay(OutboxRelay):
	"""Relay used in tests: does nothing instead of polling/forwarding to Kafka."""

	async def run(self) -> None:
		return None

	async def _tick(self) -> None:
		return None


async def _dummy_consumer_iterator() -> AsyncIterator[dict[str, str]]:
	yield {'chat_oid': 'dummy-chat-oid', 'message': 'dummy message'}
	await asyncio.sleep(1)


async def _dummy_start_consuming(topic: str) -> AsyncIterator[dict[str, str]]:
	async for message in _dummy_consumer_iterator():
		yield message


class DummyMessageBroker:
	async def start(self) -> None:
		pass

	async def close(self) -> None:
		pass

	async def send_message(self, topic: str, key: bytes, value: bytes) -> None:
		pass

	def start_consuming(self, topic: str) -> AsyncIterator[dict[str, str]]:
		return _dummy_start_consuming(topic)

	def stop_consuming(self) -> None:
		pass


def init_dummy_container(*, wrap_repos_with_breaker: bool = False) -> Container:
	container = init_container()

	# Opt-in (used by the 503 integration test): wrap the in-memory repos in the
	# same breaker proxies the production container wires (logic/init.py). Memory
	# repos never fail, so the breaker stays closed unless a test forces it open.
	breaker = container.resolve(CircuitBreaker)
	chats_repository: BaseChatsRepository = MemoryChatRepository()
	messages_repository: BaseMessagesRepository = MemoryMessagesRepository()
	if wrap_repos_with_breaker:
		chats_repository = CircuitBreakerChatsRepository(inner=chats_repository, breaker=breaker)
		messages_repository = CircuitBreakerMessagesRepository(inner=messages_repository, breaker=breaker)

	container.register(BaseChatsRepository, instance=chats_repository, scope=Scope.singleton)
	container.register(BaseMessagesRepository, instance=messages_repository, scope=Scope.singleton)
	container.register(
		BaseMessageBroker,
		instance=DummyMessageBroker(),
		scope=Scope.singleton,
	)
	container.register(BaseOutboxRepository, MemoryOutboxRepository, scope=Scope.singleton)
	container.register(SessionProvider, instance=_NullSessionProvider(), scope=Scope.singleton)
	container.register(
		OutboxRelay,
		instance=_NoopOutboxRelay(
			outbox_repository=container.resolve(BaseOutboxRepository),
			message_broker=container.resolve(BaseMessageBroker),
		),
		scope=Scope.singleton,
	)

	# `init_container` is @lru_cache(1) and resolves these singletons (e.g. while
	# wiring the mediator), caching the production instances in container._singletons.
	# punq consults that cache before registrations, so the overrides above would be
	# shadowed by the already-cached Mongo instances. Drop the cached singletons so
	# the next resolve rebuilds them from the overridden registrations.
	for key in (
		BaseChatsRepository,
		BaseMessagesRepository,
		BaseMessageBroker,
		BaseOutboxRepository,
		SessionProvider,
		OutboxRelay,
	):
		container._singletons.pop(key, None)  # type: ignore[call-overload]  # punq internals: untyped dict

	# Rebuild the mediator against the overridden (in-memory) container so its
	# command handlers are wired to the memory repositories.
	mediator = build_mediator(container, container.resolve(Config))
	container._singletons.pop(Mediator, None)  # type: ignore[call-overload]  # punq internals: untyped dict
	container._singletons.pop(EventMediator, None)  # type: ignore[call-overload]  # punq internals: untyped dict
	container.register(Mediator, instance=mediator, scope=Scope.singleton)
	container.register(EventMediator, instance=mediator, scope=Scope.singleton)

	return container

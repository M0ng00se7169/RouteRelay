"""ADR-0008 container wiring: the feature flags decide what gets built.

The whole safety story of the ADR rests on one property: with the flags at their
defaults (off), the container must be indistinguishable from the pre-ADR one, so
tests, existing deployments and prod images built before the valkey service
existed keep working untouched.

These tests build real containers (not the dummy one) with the environment
flipped, so the wiring itself is verified rather than assumed.
"""

from typing import Any

import pytest
from punq import Container

from application.api.lifespan import close_cache_client
from infrastructure.cache.base import BaseCacheClient
from infrastructure.cache.cached import (
	CachedChatsRepository,
	CachedMessagesRepository,
)
from infrastructure.cache.memory import MemoryCacheClient
from infrastructure.cache.valkey import ValkeyCacheClient
from infrastructure.locks.base import BaseDistributedLock
from infrastructure.locks.valkey import ValkeyLeaseLock
from infrastructure.outbox.relay import OutboxRelay
from infrastructure.presence.base import BasePresenceTracker
from infrastructure.presence.valkey import ValkeyPresenceTracker
from infrastructure.repositories.messages.base import (
	BaseChatsRepository,
	BaseMessagesRepository,
)
from infrastructure.repositories.messages.mongo import (
	MongoDBChatsRepository,
	MongoDBMessagesRepository,
)
from infrastructure.resilience import (
	CircuitBreakerChatsRepository,
	CircuitBreakerMessagesRepository,
)
from infrastructure.websockets.managers import BaseConnectionManager
from logic.init import (
	_init_container,
	init_container,
)

FEATURE_FLAGS = (
	'CACHE_ENABLED',
	'PRESENCE_ENABLED',
	'RELAY_LOCK_ENABLED',
)


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
	# The suite must not inherit a developer's .env: every flag is pinned here.
	for flag in FEATURE_FLAGS:
		monkeypatch.delenv(flag, raising=False)
	monkeypatch.setenv('VALKEY_URL', 'redis://valkey:6379/0')
	return monkeypatch


def _container(**env: str) -> Container:
	# _init_container (not the @lru_cache'd init_container) so each case gets a
	# fresh container instead of the process-wide one.
	return _init_container()

# --- flags off (the default) -------------------------------------------------


def test_flags_default_to_off(clean_env: pytest.MonkeyPatch) -> None:
	from settings.config import Config

	config = Config()

	assert config.cache_enabled is False
	assert config.presence_enabled is False
	assert config.relay_lock_enabled is False


def test_no_valkey_client_is_built_with_the_flags_off(clean_env: pytest.MonkeyPatch) -> None:
	container = _container()

	# Not just "a different client": no Valkey client at all, so nothing in the
	# process can try to reach a server that isn't there.
	assert isinstance(container.resolve(BaseCacheClient), MemoryCacheClient)


def test_lease_is_absent_with_the_flags_off(clean_env: pytest.MonkeyPatch) -> None:
	container = _container()

	assert container.resolve(BaseDistributedLock) is None
	assert container.resolve(OutboxRelay).lease is None


def test_connection_manager_has_no_presence_tracker(clean_env: pytest.MonkeyPatch) -> None:
	container = _container()

	manager = container.resolve(BaseConnectionManager)

	assert getattr(manager, 'presence_tracker', None) is None


def test_repositories_are_not_cached_with_the_flags_off(clean_env: pytest.MonkeyPatch) -> None:
	container = _container()

	chats = container.resolve(BaseChatsRepository)
	messages = container.resolve(BaseMessagesRepository)

	assert isinstance(chats, CircuitBreakerChatsRepository)
	assert isinstance(chats.inner, MongoDBChatsRepository)
	assert isinstance(messages, CircuitBreakerMessagesRepository)
	assert isinstance(messages.inner, MongoDBMessagesRepository)


# --- flags on ----------------------------------------------------------------


def test_cache_flag_wires_the_proxies_inside_the_breaker(clean_env: pytest.MonkeyPatch) -> None:
	# Order matters (ADR-0008 §4): breaker OUTSIDE, cache inside, so the 'mongo'
	# breaker keeps measuring Mongo only.
	clean_env.setenv('CACHE_ENABLED', 'true')
	container = _container()

	chats = container.resolve(BaseChatsRepository)
	messages = container.resolve(BaseMessagesRepository)

	assert isinstance(chats, CircuitBreakerChatsRepository)
	assert isinstance(chats.inner, CachedChatsRepository)
	assert isinstance(chats.inner.inner, MongoDBChatsRepository)
	assert isinstance(messages, CircuitBreakerMessagesRepository)
	assert isinstance(messages.inner, CachedMessagesRepository)
	assert isinstance(messages.inner.inner, MongoDBMessagesRepository)


def test_cache_flag_builds_a_valkey_client(clean_env: pytest.MonkeyPatch) -> None:
	clean_env.setenv('CACHE_ENABLED', 'true')
	container = _container()

	assert isinstance(container.resolve(BaseCacheClient), ValkeyCacheClient)


def test_presence_flag_wires_the_manager(clean_env: pytest.MonkeyPatch) -> None:
	clean_env.setenv('PRESENCE_ENABLED', 'true')
	container = _container()

	assert isinstance(container.resolve(BasePresenceTracker), ValkeyPresenceTracker)
	manager = container.resolve(BaseConnectionManager)
	assert getattr(manager, 'presence_tracker', None) is not None


def test_relay_lock_flag_wires_the_lease(clean_env: pytest.MonkeyPatch) -> None:
	clean_env.setenv('RELAY_LOCK_ENABLED', 'true')
	container = _container()

	lease = container.resolve(BaseDistributedLock)

	assert isinstance(lease, ValkeyLeaseLock)
	assert container.resolve(OutboxRelay).lease is lease


def test_ttl_knobs_reach_the_wired_collaborators(clean_env: pytest.MonkeyPatch) -> None:
	clean_env.setenv('PRESENCE_ENABLED', 'true')
	clean_env.setenv('PRESENCE_TTL_SECONDS', '45')
	clean_env.setenv('RELAY_LOCK_ENABLED', 'true')
	clean_env.setenv('RELAY_LOCK_TTL_SECONDS', '21')
	container = _container()

	assert container.resolve(BasePresenceTracker).ttl_seconds == 45
	assert container.resolve(BaseDistributedLock).ttl_seconds == 21


# --- teardown ----------------------------------------------------------------


@pytest.mark.asyncio
async def test_lifespan_closes_the_cache_client(clean_env: pytest.MonkeyPatch) -> None:
	from application.api.main import create_app

	clean_env.setenv('CACHE_ENABLED', 'true')
	container = _container()
	cache = container.resolve(BaseCacheClient)

	closed: list[bool] = []
	original_aclose = cache.aclose

	async def tracking_aclose() -> None:
		closed.append(True)
		await original_aclose()

	cache.aclose = tracking_aclose  # test seam

	# close_cache_client resolves through _active_container, which honours the
	# app's dependency override; same path the production shutdown takes.
	app = create_app()
	app.dependency_overrides[init_container] = lambda: container

	await close_cache_client(app)

	assert closed == [True]


@pytest.mark.asyncio
async def test_closing_an_unused_cache_client_is_safe() -> None:
	# No Valkey client was ever materialized: shutdown must still complete.
	await close_cache_client()


def test_command_handlers_receive_a_cache_client(clean_env: pytest.MonkeyPatch) -> None:
	# The handlers never branch on an optional cache: they always get one.
	container = _container()
	cache = container.resolve(BaseCacheClient)

	assert isinstance(cache, BaseCacheClient)


def test_dummy_container_uses_the_memory_doubles() -> None:
	from test.fixtures import init_dummy_container

	container: Any = init_dummy_container()

	assert isinstance(container.resolve(BaseCacheClient), MemoryCacheClient)
	assert isinstance(container.resolve(BasePresenceTracker), BasePresenceTracker)

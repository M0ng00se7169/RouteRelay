"""Circuit breaker (infrastructure/resilience.py).

Unit tests for the state machine (closed -> open -> half-open -> closed) and
for the repository proxies that guard Mongo calls. Metric assertions follow the
baseline-delta house pattern (the prometheus registry is global across tests),
and each test uses a unique breaker ``name`` so gauge/counter children never
collide between tests.
"""

from time import monotonic
from typing import cast

import pytest
from prometheus_client import REGISTRY

from domain.entities.messages import Chat, Message
from infrastructure.repositories.filters.messages import GetMessagesFilters
from infrastructure.repositories.messages.base import (
    BaseChatsRepository,
    BaseMessagesRepository,
)
from infrastructure.resilience import (
    CircuitBreaker,
    CircuitBreakerChatsRepository,
    CircuitBreakerMessagesRepository,
    CircuitOpenError,
)


class FlakyDependency:
    """Fake remote dependency: raises ``error`` on every call until told not to."""

    def __init__(self, error: Exception | None = RuntimeError('dependency down')):
        self.error = error
        self.calls = 0

    async def __call__(self) -> str:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return 'ok'


def _rejected(name: str) -> float:
    sample = REGISTRY.get_sample_value('circuit_breaker_rejected_total', {'name': name})
    return sample if sample is not None else 0.0


def _state(name: str) -> float:
    sample = REGISTRY.get_sample_value('circuit_breaker_state', {'name': name})
    return sample if sample is not None else 0.0


@pytest.mark.asyncio
async def test_success_passes_through_and_stays_closed() -> None:
    breaker = CircuitBreaker(name='unit-success', failure_threshold=2, recovery_time=30)
    dep = FlakyDependency(error=None)

    assert await breaker.call(dep) == 'ok'
    assert breaker.state == 'closed'
    assert _state('unit-success') == 0.0


@pytest.mark.asyncio
async def test_breaker_opens_after_threshold_then_fails_fast() -> None:
    breaker = CircuitBreaker(name='unit-open', failure_threshold=3, recovery_time=60)
    dep = FlakyDependency()

    for _ in range(3):
        with pytest.raises(RuntimeError, match='dependency down'):
            await breaker.call(dep)
    assert breaker.state == 'open'
    assert _state('unit-open') == 1.0

    # Fail fast: further calls are rejected without touching the dependency.
    with pytest.raises(CircuitOpenError):
        await breaker.call(dep)
    assert dep.calls == 3
    assert _rejected('unit-open') == 1.0


@pytest.mark.asyncio
async def test_consecutive_not_cumulative_failures() -> None:
    # A single success between failures resets the consecutive-failure count.
    breaker = CircuitBreaker(name='unit-consecutive', failure_threshold=3, recovery_time=60)
    dep = FlakyDependency()

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(dep)
    dep.error = None
    await breaker.call(dep)
    dep.error = RuntimeError('dependency down')
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(dep)

    # Only 2 consecutive failures -> still closed.
    assert breaker.state == 'closed'


@pytest.mark.asyncio
async def test_half_open_probe_failure_reopens() -> None:
    breaker = CircuitBreaker(name='unit-halfopen-fail', failure_threshold=1, recovery_time=30)
    dep = FlakyDependency()

    with pytest.raises(RuntimeError):
        await breaker.call(dep)
    assert breaker.state == 'open'

    # Simulate recovery_time elapsing -> next call is the probe.
    breaker._opened_at = monotonic() - breaker.recovery_time - 1
    with pytest.raises(RuntimeError):
        await breaker.call(dep)

    # The probe failed -> breaker is open again, no further probes allowed.
    assert breaker.state == 'open'
    with pytest.raises(CircuitOpenError):
        await breaker.call(dep)
    assert dep.calls == 2


@pytest.mark.asyncio
async def test_probe_failure_reopens_even_with_high_threshold() -> None:
	# Regression: with failure_threshold > 1, a failed probe must reopen the
	# breaker immediately. Rerouting the probe failure through the consecutive
	# failure count would leave the breaker stuck half-open forever (the probe
	# flag is never cleared and the threshold is never reached again).
	breaker = CircuitBreaker(name='unit-probe-deadlock', failure_threshold=3, recovery_time=30)
	dep = FlakyDependency()

	for _ in range(3):
		with pytest.raises(RuntimeError):
			await breaker.call(dep)
	assert breaker.state == 'open'

	# Recovery window elapses -> probe is allowed and fails.
	breaker._opened_at = monotonic() - breaker.recovery_time - 1
	with pytest.raises(RuntimeError):
		await breaker.call(dep)

	# Reopened (not stuck half-open): calls fail fast, and after another
	# recovery window a new probe is permitted.
	assert breaker.state == 'open'
	with pytest.raises(CircuitOpenError):
		await breaker.call(dep)

	breaker._opened_at = monotonic() - breaker.recovery_time - 1
	dep.error = None
	assert await breaker.call(dep) == 'ok'
	assert breaker.state == 'closed'


@pytest.mark.asyncio
async def test_half_open_probe_success_closes() -> None:
    breaker = CircuitBreaker(name='unit-halfopen-ok', failure_threshold=1, recovery_time=30)
    dep = FlakyDependency()

    with pytest.raises(RuntimeError):
        await breaker.call(dep)
    breaker._opened_at = monotonic() - breaker.recovery_time - 1

    dep.error = None
    assert await breaker.call(dep) == 'ok'
    assert breaker.state == 'closed'
    assert _state('unit-halfopen-ok') == 0.0

    # Closed again: calls go through normally.
    assert await breaker.call(dep) == 'ok'
    assert dep.calls == 3


@pytest.mark.asyncio
async def test_circuit_open_error_is_not_counted_as_failure() -> None:
    # A rejection must not feed back into the failure count (it would keep the
    # breaker open forever after recovery).
    breaker = CircuitBreaker(name='unit-no-feedback', failure_threshold=2, recovery_time=30)
    dep = FlakyDependency()

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(dep)

    for _ in range(5):
        with pytest.raises(CircuitOpenError):
            await breaker.call(dep)

    # Probe after recovery succeeds -> breaker closes despite the rejections.
    breaker._opened_at = monotonic() - breaker.recovery_time - 1
    dep.error = None
    assert await breaker.call(dep) == 'ok'
    assert breaker.state == 'closed'


# --- repository proxies ------------------------------------------------------


class FailingChatsRepo:
    """Minimal chats repo stand-in whose reads always fail.

    Deliberately duck-typed (only the methods these tests touch); the casts at
    the proxy construction sites bridge that to the ABC-typed ``inner`` field.
    """

    def __init__(self) -> None:
        self.calls = 0

    async def get_chat_by_oid(self, oid: str) -> Chat | None:
        self.calls += 1
        raise RuntimeError('mongo is down')


class MemoryChatsRepoStub:
    def __init__(self) -> None:
        self.seen: list[str] = []

    async def get_chat_by_oid(self, oid: str) -> Chat | None:
        self.seen.append(oid)
        return cast(Chat, {'oid': oid})


@pytest.mark.asyncio
async def test_chats_proxy_delegates_and_guards() -> None:
    breaker = CircuitBreaker(name='unit-proxy', failure_threshold=2, recovery_time=60)
    # cast: the stub implements only the exercised subset of the ABC.
    repo = CircuitBreakerChatsRepository(
        inner=cast(BaseChatsRepository, MemoryChatsRepoStub()),
        breaker=breaker,
    )

    assert await repo.get_chat_by_oid(oid='c1') == {'oid': 'c1'}

    failing = FailingChatsRepo()
    repo.inner = cast(BaseChatsRepository, failing)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await repo.get_chat_by_oid(oid='c2')
    with pytest.raises(CircuitOpenError):
        await repo.get_chat_by_oid(oid='c2')
    assert failing.calls == 2


@pytest.mark.asyncio
async def test_messages_proxy_guards_get_messages() -> None:
    breaker = CircuitBreaker(name='unit-proxy-msgs', failure_threshold=1, recovery_time=60)

    class FailingMessagesRepo:
        async def get_messages(
            self,
            chat_oid: str,
            filters: GetMessagesFilters,
        ) -> tuple[list[Message], int]:
            raise RuntimeError('mongo is down')

    repo = CircuitBreakerMessagesRepository(
        inner=cast(BaseMessagesRepository, FailingMessagesRepo()),
        breaker=breaker,
    )

    with pytest.raises(RuntimeError):
        await repo.get_messages(chat_oid='c1', filters=GetMessagesFilters())
    with pytest.raises(CircuitOpenError):
        await repo.get_messages(chat_oid='c1', filters=GetMessagesFilters())
